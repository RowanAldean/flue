import { SUBMISSION_HARNESS_NAME, SUBMISSION_SESSION_NAME } from '../adapter-helpers.ts';
import type { AgentSubmission, AgentSubmissionStore } from '../agent-execution-store.ts';
import type { FlueContextInternal } from '../client.ts';
import { ConversationRecordWriter } from '../conversation-writer.ts';
import {
	AgentInstanceExistsError,
	AgentInstanceNotFoundError,
	classifyError,
	InvalidRequestError,
	SubmissionAbortedError,
	SubmissionConflictError,
	SubmissionTimeoutError,
} from '../errors.ts';
import { interceptExecution } from '../execution-interceptor.ts';
import { createMcpConnectionCache } from '../mcp.ts';
import {
	type AttachedAgentSubmissionOptions,
	admitInstanceContact,
	adoptKeyedSubmissionReplay,
	type createAgentSubmissionSessionHandler,
	createDirectAgentSubmissionInput,
	createDispatchAgentSubmissionInput,
	ensureInstanceIdentity,
	finalizePendingSettlement,
	type InstanceContactAdmission,
	type InstanceIdentity,
	isInstanceContactRejection,
	materializeSubmissionAttachments,
	processSubmission,
	reconcileInterruptedSubmission,
	serializeSubmissionError,
	settleUnclaimableSubmission,
	submissionSyntheticRequest,
	unreadySubmissionDeadline,
} from '../runtime/agent-submissions.ts';
import type { AttachmentStore } from '../runtime/attachment-store.ts';
import type { ConversationStreamStore } from '../runtime/conversation-stream-store.ts';
import {
	type CoordinatorEventEmitter,
	createCoordinatorEventEmitter,
	drainGlobalEventDeliveries,
} from '../runtime/events.ts';
import { assertAgentDispatchAdmissionInput, handleAgentRequest } from '../runtime/handle-agent.ts';
import {
	handleAgentAttachmentRead,
	handleAgentConversationHead,
	handleAgentConversationRead,
} from '../runtime/handle-conversation-routes.ts';
import { generateAttemptId, isKeyDerivedSubmissionId } from '../runtime/ids.ts';
import { agentStreamPath } from '../runtime/stream-offsets.ts';
import { createSessionStorageKey } from '../session-identity.ts';
import type { DeliveredMessage } from '../types.ts';
import {
	createSqlAgentExecutionStore,
	createSqlConversationStores,
} from './agent-execution-store.ts';

export const CLOUDFLARE_AGENT_INTERNAL_DISPATCH_PATH = '/__flue/internal/dispatch';
export const CLOUDFLARE_AGENT_INTERNAL_INSTANCE_INFO_PATH = '/__flue/internal/instance-info';

/**
 * The two Agents SDK Task definitions the generated Durable Object class
 * declares on `taskDefinitions` (see `flue-agent-class.ts`). The SDK persists
 * only the name per run and resolves it against the class on every wake, so
 * an in-flight run always finds the same handler.
 *
 * `flue:drive@v1` is the supervisor pass: reconcile durable submission state
 * and start an attempt run for every claimable head. One run per object,
 * joined (not duplicated) by everyone who needs the queue to progress.
 *
 * `flue:attempt@v1` is one submission's processing, awaited in the handler
 * body. One run per submission; the SDK replays it after an interruption and
 * enforces its deadline over a hung attempt.
 */
export const FLUE_DRIVE_TASK = 'flue:drive@v1';
export const FLUE_ATTEMPT_TASK = 'flue:attempt@v1';
const FLUE_DRIVE_RUN_ID = 'flue:drive';

function attemptRunId(submissionId: string): string {
	return `flue:submission:${submissionId}`;
}

import type { SqlStorage } from '../sql-storage.ts';

interface CloudflareAgentStorage {
	sql?: SqlStorage;
	transactionSync?<T>(closure: () => T): T;
}

/** The slice of an Agents SDK Task run snapshot the coordinator reads. */
interface CloudflareTaskRunSnapshot {
	readonly state: 'pending' | 'running' | 'waiting' | 'completed' | 'failed' | 'cancelled';
}

/**
 * The slice of the Agents SDK `Tasks` capability the coordinator drives.
 * Structural, like the rest of this file: `@flue/runtime` never imports
 * `agents`; the generated entry supplies the class.
 */
interface CloudflareAgentTasks {
	run(
		definition: string,
		input: unknown,
		options: { runId: string; retain: false; deadline?: number },
	): Promise<{ accepted: boolean }>;
	get(runId: string): Promise<CloudflareTaskRunSnapshot | null>;
}

interface CloudflareAgentInstance {
	readonly name: string;
	readonly env: Record<string, unknown>;
	readonly ctx: {
		readonly id: { toString(): string };
		readonly storage: CloudflareAgentStorage;
		/**
		 * DurableObjectState.waitUntil. Optional because test fakes may omit
		 * it; it only tells the platform that fire-and-forget event delivery
		 * left behind by an invocation is deliberate.
		 */
		waitUntil?(promise: Promise<unknown>): void;
	};
	readonly tasks: CloudflareAgentTasks;
}

/** The slice of an Agents SDK `TaskStep` the Task handler bodies read. */
export interface CloudflareTaskStep {
	/** Aborts for the whole attempt: on `cancel()` and at the run deadline. */
	readonly signal: AbortSignal;
}

/** Input of one `flue:attempt@v1` run. */
export interface CloudflareAttemptTaskInput {
	readonly submissionId: string;
}

interface CloudflareAgentPreparedCoordinator {
	readonly agentName: string;
	readonly submissionStore: AgentSubmissionStore;
	readonly conversationStreamStore: ConversationStreamStore;
	readonly attachmentStore: AttachmentStore;
}

interface CloudflareAgentRuntimeOptions {
	readonly agents: ReadonlyArray<{
		readonly name: string;
		readonly agent: Parameters<typeof createAgentSubmissionSessionHandler>[0];
	}>;
	readonly createContext: (options: {
		readonly submissionStore: AgentSubmissionStore;
		readonly instance: CloudflareAgentInstance;
		readonly agentName: string;
		readonly request: Request;
		readonly submissionId?: string;
	}) => FlueContextInternal;
	readonly runWithInstanceContext: <T>(
		instance: CloudflareAgentInstance,
		agentName: string,
		callback: () => T,
	) => T;
}

export interface CloudflareAgentRuntime {
	prepare(options: {
		readonly storage: CloudflareAgentStorage;
		readonly className: string;
		readonly agentName: string;
	}): CloudflareAgentPreparedCoordinator;
	attach(instance: CloudflareAgentInstance, prepared: CloudflareAgentPreparedCoordinator): void;
	onStart(
		instance: CloudflareAgentInstance,
		inherited: () => Promise<unknown> | unknown,
	): Promise<void>;
	/**
	 * Handler of the `flue:drive@v1` Task: one bounded, storage-only
	 * supervisor pass that reconciles durable submission state and starts an
	 * attempt run for every claimable head, then completes. The single place
	 * submission attempts start; every other boundary only records durable
	 * intent and ensures a drive run exists.
	 */
	drive(instance: CloudflareAgentInstance, step: CloudflareTaskStep): Promise<void>;
	/**
	 * Handler of the `flue:attempt@v1` Task: process one submission to
	 * settlement in the handler body. The SDK replays it on a fresh isolate
	 * after an interruption (the body reconciles from durable evidence
	 * first) and enforces the submission's durability timeout as the run's
	 * deadline, settling over an attempt that ignores its signal.
	 */
	attempt(
		instance: CloudflareAgentInstance,
		input: CloudflareAttemptTaskInput,
		step: CloudflareTaskStep,
	): Promise<void>;
	/**
	 * The SDK recorded a terminal Task failure without running (or over) a
	 * handler — a deadline, an exhausted budget, a missing definition. The
	 * submission row it owned is still unsettled: ensure a drive run so the
	 * reconcile pass settles it from evidence.
	 */
	onTaskError(instance: CloudflareAgentInstance, error: unknown): Promise<void>;
	onRequest(instance: CloudflareAgentInstance, request: Request): Promise<Response | null>;
	/**
	 * Run the Agents SDK alarm handler inside the instance context. Alarms
	 * dispatch `schedule`/`scheduleEvery`/`queue` callbacks to methods on the
	 * (possibly extension-authored) class, so this is the boundary that gives
	 * user scheduled callbacks `getCloudflareContext()` and
	 * `getDurableObjectIdentity()`.
	 */
	onAlarm(
		instance: CloudflareAgentInstance,
		inherited: () => Promise<unknown> | unknown,
	): Promise<unknown>;
}

export function createCloudflareAgentRuntime(
	options: CloudflareAgentRuntimeOptions,
): CloudflareAgentRuntime {
	const coordinators = new WeakMap<CloudflareAgentInstance, CloudflareAgentCoordinator>();

	const getCoordinator = (instance: CloudflareAgentInstance): CloudflareAgentCoordinator => {
		const coordinator = coordinators.get(instance);
		if (!coordinator) {
			throw new Error('[flue] Generated Cloudflare agent coordinator was not initialized.');
		}
		return coordinator;
	};

	return {
		prepare({ storage, className, agentName }) {
			const submissionStore = createSqlAgentExecutionStore(storage, className);
			const conversationStores = createSqlConversationStores(storage, className);
			return {
				agentName,
				submissionStore,
				...conversationStores,
			};
		},
		attach(instance, prepared) {
			coordinators.set(instance, new CloudflareAgentCoordinator(instance, prepared, options));
		},
		onStart(instance, inherited) {
			return getCoordinator(instance).onStart(inherited);
		},
		drive(instance, step) {
			return getCoordinator(instance).drive(step);
		},
		attempt(instance, input, step) {
			return getCoordinator(instance).attempt(input, step);
		},
		onTaskError(instance, error) {
			return getCoordinator(instance).onTaskError(error);
		},
		onRequest(instance, request) {
			return getCoordinator(instance).onRequest(request);
		},
		onAlarm(instance, inherited) {
			return getCoordinator(instance).onAlarm(inherited);
		},
	};
}

class CloudflareAgentCoordinator {
	constructor(
		private readonly instance: CloudflareAgentInstance,
		private readonly prepared: CloudflareAgentPreparedCoordinator,
		private readonly options: CloudflareAgentRuntimeOptions,
	) {
		this.emitCoordinatorEvent = createCoordinatorEventEmitter({
			agentName: prepared.agentName,
			instanceId: instance.name,
			env: instance.env,
		});
	}

	private conversationWriter: ConversationRecordWriter | undefined;
	private conversationWriterCreation: Promise<ConversationRecordWriter> | undefined;
	private conversationMaterialization: Promise<void> = Promise.resolve();
	/**
	 * Context-free live event emitter for coordinator signals
	 * (`submission_queued`, `submission_recovery`, recovered settlements) —
	 * independent of context/writer creation, which is among the failures it
	 * reports, and infallible by contract so it can never worsen a recovery
	 * catch block.
	 */
	private readonly emitCoordinatorEvent: CoordinatorEventEmitter;
	/**
	 * Live MCP connections for this instance (one DO = one agent instance).
	 * Submissions reuse them while the isolate stays warm; eviction is the
	 * teardown — a DO has no disposal hook, and streamable HTTP holds no
	 * server state worth a farewell.
	 */
	private readonly mcpConnections = createMcpConnectionCache();
	/**
	 * Abort controllers for attempts live in this isolate, keyed by
	 * submissionId, so an incoming cancel request can abort the running
	 * attempt. The DO is single-threaded but interleaves at `await` points,
	 * so a cancel request can fire the controller while the attempt is
	 * suspended on provider I/O. If the isolate is evicted the controller is
	 * gone and the abort falls back to the durable `abortRequestedAt` +
	 * reconcile path.
	 */
	private activeControllers = new Map<string, AbortController>();
	/**
	 * Attempt ids this isolate claimed and handed to an attempt run that has
	 * not yet entered its handler body. The run's first execution takes the
	 * id out and processes the claim as-is; a body that finds no entry is a
	 * replay on a fresh isolate (or after its previous execution ended) and
	 * must reconcile the row from durable evidence before doing anything.
	 */
	private readonly startedAttempts = new Set<string>();

	// Instance context is established at the boundaries where execution
	// enters the Durable Object: onStart, onRequest, onAlarm, and the two
	// Task handler bodies (drive, attempt). A Task handler may run on a fresh
	// isolate with no ambient context, so each body (re)establishes it.
	// onAlarm wraps the Agents SDK alarm handler, covering every scheduled
	// callback it dispatches — including extension-authored
	// schedule/scheduleEvery/queue targets (#437). Everything reachable from
	// these boundaries — dispatch admission, reconciliation, materialization,
	// submission processing — assumes the context is already present and
	// never re-wraps.
	//
	// Execution ownership: attempts start ONLY from the drive run's
	// reconcile pass, each as its own `flue:attempt@v1` Task run whose
	// handler body awaits the submission to settlement. The Agents SDK owns
	// the durable wake (a claim backstop while the run is held, a replay on
	// a fresh isolate after an interruption), the deadline (the submission's
	// durability timeout, settled over a hung attempt with its later writes
	// fenced out), and the attempt-wide abort signal. All other boundaries
	// (admission, abort, onStart, Task failures) record durable intent and
	// ensure the drive run exists; joining an existing one is free.
	onStart(inherited: () => Promise<unknown> | unknown): Promise<void> {
		return this.runWithInstanceContext(async () => {
			// A fresh isolate has no live attempt by definition, so unsettled
			// work needs nothing beyond a drive: its reconcile pass classifies
			// interrupted attempts directly. Ensure it before the (possibly
			// extension-authored) inherited onStart — the durable driver must
			// be in place even if extension startup throws.
			await this.ensureDriveIfUnsettled();
			await inherited();
		});
	}

	/**
	 * The `flue:drive@v1` handler: one bounded supervisor pass. Reconcile
	 * durable state (settlement finalization, interrupted-attempt recovery,
	 * runnable claims) and start an attempt run per claim WITHOUT awaiting
	 * it — attempt runs settle their submissions themselves and ensure the
	 * next drive when queued work remains. The pass never waits on agent
	 * execution, so it completes in bounded time whatever any attempt is
	 * doing.
	 */
	drive(step: CloudflareTaskStep): Promise<void> {
		return this.runWithInstanceContext(async () => {
			if (step.signal.aborted) return;
			if (!(await this.submissions.hasUnsettledSubmissions())) return;
			// The reconcile pass is storage-only and runs under the
			// `flue.coordinator` interception so tracing backends can group
			// its platform-instrumented storage spans. Attempt runs start
			// AFTER the interception settles, deliberately: a run warm-started
			// inside the span's activation would re-parent its invoke_agent
			// span under the coordinator span. A claim whose start is
			// preempted here (crash, code-update reset) is a running row with
			// no attempt run — the next drive's running-recovery loop
			// reconciles it, and a durable abort in the claim-to-start gap
			// resolves through the existing abortRequestedAt path.
			const claims = await interceptExecution(
				{ type: 'coordinator', phase: 'reconcile' },
				{ instanceId: this.instance.name, agentName: this.agentName },
				() => this.reconcileSubmissions(),
			);
			for (const claimed of claims) {
				try {
					await this.ensureAttemptRun(claimed);
				} catch (error) {
					this.logSubmissionReconciliationFailure(claimed, 'start_submission', error);
				}
			}
			// observe() deliveries are fire-and-forget on the emit path; hand
			// whatever this pass emitted (recovered settlements) to the
			// platform so the invocation's end can't tear them down mid-POST.
			this.instance.ctx.waitUntil?.(drainGlobalEventDeliveries());
		});
	}

	/**
	 * The `flue:attempt@v1` handler: process one submission to settlement.
	 *
	 * First execution of a claim this isolate made: process it as-is. Any
	 * other entry — a replay on a fresh isolate, or after the previous
	 * execution ended — is an interrupted attempt: reconcile the row from
	 * durable evidence, which settles it or claims a replacement attempt to
	 * process. A row that is no longer running, or that another live attempt
	 * already owns, is left alone. Never throws for application failures:
	 * `processSubmission` settles them durably; a throw here is a platform
	 * failure the SDK replays.
	 */
	attempt(input: CloudflareAttemptTaskInput, step: CloudflareTaskStep): Promise<void> {
		return this.runWithInstanceContext(async () => {
			const { submissionId } = input;
			// Two passes at most: a lost reconcile race re-reads once, and a
			// row that is then still unclaimed belongs to whoever won it.
			for (let pass = 0; pass < 2; pass++) {
				if (step.signal.aborted) return;
				const row = await this.submissions.getSubmission(submissionId);
				if (!row || row.status !== 'running' || !row.attemptId) return;
				if (this.activeControllers.has(submissionId)) return;
				let claimed: AgentSubmission | undefined;
				if (this.startedAttempts.delete(row.attemptId)) {
					claimed = row;
				} else {
					claimed = await this.reconcileInterruptedSubmission(row);
					if (!claimed?.attemptId) continue;
					this.startedAttempts.delete(claimed.attemptId);
				}
				await this.runAttempt(claimed, step.signal);
				return;
			}
		});
	}

	/**
	 * Run one claimed attempt in the current handler body, its flue abort
	 * controller linked to the Task's attempt-wide signal (an SDK
	 * cancellation or the run deadline unwinds it), and ensure the next
	 * drive when the submission settled with queued work behind it. Never
	 * rejects for application failures — `processSubmission` settles them
	 * durably; a platform failure propagates so the SDK replays the run.
	 */
	private async runAttempt(submission: AgentSubmission, taskSignal: AbortSignal): Promise<void> {
		const controller = new AbortController();
		this.activeControllers.set(submission.submissionId, controller);
		const onTaskAbort = () => controller.abort(submissionAbortReason(taskSignal.reason));
		if (taskSignal.aborted) onTaskAbort();
		else taskSignal.addEventListener('abort', onTaskAbort, { once: true });
		try {
			await this.processSubmissionEntry(submission, controller.signal);
		} finally {
			taskSignal.removeEventListener('abort', onTaskAbort);
			this.deleteControllerIfCurrent(submission.submissionId, controller);
			try {
				const settled =
					(await this.submissions.getSubmission(submission.submissionId))?.status === 'settled';
				if (settled && (await this.submissions.hasUnsettledSubmissions())) {
					await this.ensureDrive();
				}
			} catch {
				// Best-effort: an attempt that ends with its submission still
				// unsettled (a failure inside settlement itself) is a running
				// row the next drive reconciles.
			}
			// The attempt's settlement events (submission_settled and any
			// subscriber bridge work they trigger) ride this waitUntil.
			this.instance.ctx.waitUntil?.(drainGlobalEventDeliveries());
		}
	}

	/**
	 * A terminal Task failure the SDK recorded — a deadline settled over a
	 * hung attempt, an exhausted budget, a definition missing after a deploy.
	 * The submission it was processing is still a running row; ensure a
	 * drive so the reconcile pass classifies it. Application failures never
	 * reach here as Task failures: the attempt body settles them itself.
	 */
	onTaskError(error: unknown): Promise<void> {
		return this.runWithInstanceContext(async () => {
			if (!isTaskRecordedFailure(error)) return;
			await this.ensureDriveIfUnsettled();
		});
	}

	onRequest(request: Request): Promise<Response | null> {
		return this.runWithInstanceContext(async () => {
			try {
				return await this.routeRequest(request);
			} finally {
				// Admission-side observe() emissions (submission_queued, abort
				// advisories) must survive the request invocation ending.
				this.instance.ctx.waitUntil?.(drainGlobalEventDeliveries());
			}
		});
	}

	async onAlarm(inherited: () => Promise<unknown> | unknown): Promise<unknown> {
		return this.runWithInstanceContext(() => inherited());
	}

	private async routeRequest(request: Request): Promise<Response | null> {
		if (isInternalDispatchRequest(request)) return this.admitDispatch(request);
		if (isInternalInstanceInfoRequest(request)) return this.instanceInfo();

		if (isAbortRequest(request, this.agentName, this.instance.name)) {
			const aborted = await this.abortInstance();
			return Response.json({ aborted });
		}

		const method = request.method;
		if (method === 'GET' || method === 'HEAD') {
			const streamPath = agentStreamPath(this.agentName, this.instance.name);
			// Attachment byte download. The outer Worker has already run the
			// module's `route` middleware, only forwards GET, and rewrites the
			// request onto the canonical `/agents/<name>/<id>/attachments/<id>`
			// path whatever the public mount looks like — so the DO, which owns
			// the bytes, just serves from its attachment store. Match the exact
			// tail (not a loose `/attachments/` substring) so an agent literally
			// named "attachments" doesn't misroute its conversation reads here.
			const segments = new URL(request.url).pathname.split('/');
			const attachmentId =
				method === 'GET' &&
				segments.length >= 4 &&
				segments[segments.length - 2] === 'attachments' &&
				segments[segments.length - 3] === this.instance.name &&
				segments[segments.length - 4] === this.agentName
					? decodeURIComponent(segments[segments.length - 1] as string)
					: undefined;
			if (attachmentId) {
				return handleAgentAttachmentRead({
					conversationStore: this.prepared.conversationStreamStore,
					attachmentStore: this.prepared.attachmentStore,
					path: streamPath,
					attachmentId: decodeURIComponent(attachmentId),
				});
			}
			if (method === 'HEAD') {
				return await handleAgentConversationHead(this.prepared.conversationStreamStore, streamPath);
			}
			return handleAgentConversationRead({
				store: this.prepared.conversationStreamStore,
				path: streamPath,
				request,
			});
		}

		return handleAgentRequest({
			request,
			id: this.instance.name,
			agentName: this.agentName,
			admitAttachedSubmission: (message, options) => this.admitAttachedSubmission(message, options),
		});
	}

	private get agentName(): string {
		return this.prepared.agentName;
	}

	private get submissions(): AgentSubmissionStore {
		return this.prepared.submissionStore;
	}

	private runWithInstanceContext<T>(callback: () => T): T {
		return this.options.runWithInstanceContext(this.instance, this.agentName, callback);
	}

	private async ensureConversationWriter(): Promise<ConversationRecordWriter> {
		if (this.conversationWriter && !this.conversationWriter.failed) return this.conversationWriter;
		if (!this.conversationWriterCreation) {
			const creation = ConversationRecordWriter.create({
				store: this.prepared.conversationStreamStore,
				path: agentStreamPath(this.agentName, this.instance.name),
				identity: { agentName: this.agentName, instanceId: this.instance.name },
				producerId: this.instance.ctx.id.toString(),
				onFailed: (writer) => {
					if (this.conversationWriter === writer) this.conversationWriter = undefined;
				},
			});
			this.conversationWriterCreation = creation;
			void creation.then(
				(writer) => {
					if (!writer.failed) this.conversationWriter = writer;
					if (this.conversationWriterCreation === creation)
						this.conversationWriterCreation = undefined;
				},
				() => {
					if (this.conversationWriterCreation === creation)
						this.conversationWriterCreation = undefined;
				},
			);
		}
		return this.conversationWriterCreation;
	}

	private createContext(request: Request, submissionId?: string): FlueContextInternal {
		return this.options.createContext({
			submissionStore: this.submissions,
			instance: this.instance,
			agentName: this.agentName,
			request,
			submissionId,
		});
	}

	private createDurableContext(request: Request, submissionId?: string): FlueContextInternal {
		const ctx = this.createContext(request, submissionId);
		ctx.setConversationWriter?.(this.conversationWriter);
		ctx.setAttachmentStore?.(this.prepared.attachmentStore);
		ctx.setMcpConnections?.(this.mcpConnections);
		return ctx;
	}

	private get tasks(): CloudflareAgentTasks {
		const tasks = this.instance.tasks;
		if (!tasks || typeof tasks.run !== 'function') {
			throw new Error(
				'[flue] The installed "agents" package does not provide the Cloudflare Agents SDK Tasks capability on Agent. Upgrade @flue/vite (which supplies the Cloudflare Agents SDK), or remove the "agents" dependency from your project if it declares an older one.',
			);
		}
		return tasks;
	}

	/**
	 * Ensure a drive run exists. One run id per object: a caller whose
	 * request lands while a drive is live joins it (`accepted: false`), and
	 * the SDK's durable acceptance is the wake that survives isolate death.
	 * A drive that completed is gone (`retain: false`), so the next ensure
	 * starts a fresh one. Warm-started in the caller's invocation whenever
	 * the object is past startup.
	 */
	private async ensureDrive(): Promise<void> {
		await this.tasks.run(FLUE_DRIVE_TASK, undefined, {
			runId: FLUE_DRIVE_RUN_ID,
			retain: false,
		});
	}

	private async ensureDriveIfUnsettled(): Promise<boolean> {
		if (!(await this.submissions.hasUnsettledSubmissions())) return false;
		await this.ensureDrive();
		return true;
	}

	/**
	 * Start (or join) the attempt run for one claimed submission. The claim's
	 * attempt id is registered before the run is accepted, because a warm
	 * start enters the handler body in this same invocation. The
	 * submission's durability timeout becomes the run deadline: the SDK
	 * wakes at it, settles the run over an attempt that ignores its signal,
	 * and fences the attempt's later Task writes; the drive then reconciles
	 * the row through its existing timeout classification.
	 */
	private async ensureAttemptRun(submission: AgentSubmission): Promise<void> {
		if (!submission.attemptId) return;
		this.startedAttempts.add(submission.attemptId);
		try {
			await this.tasks.run(
				FLUE_ATTEMPT_TASK,
				{ submissionId: submission.submissionId } satisfies CloudflareAttemptTaskInput,
				{
					runId: attemptRunId(submission.submissionId),
					retain: false,
					...(submission.timeoutAt > 0 ? { deadline: submission.timeoutAt } : {}),
				},
			);
		} catch (error) {
			this.startedAttempts.delete(submission.attemptId);
			throw error;
		}
	}

	/** Whether the SDK still holds a non-terminal attempt run for this submission. */
	private async hasLiveAttemptRun(submissionId: string): Promise<boolean> {
		const run = await this.tasks.get(attemptRunId(submissionId));
		return (
			run !== null &&
			(run.state === 'pending' || run.state === 'running' || run.state === 'waiting')
		);
	}

	/**
	 * One reconcile pass: materialize unready submissions, finalize pending
	 * settlements, recover interrupted attempts, enforce deadlines on live
	 * ones, and claim runnable work. Returns the claims for the supervisor
	 * pass to start — this method never waits on agent execution itself.
	 * Failures are logged with `deferred_to_scheduled_wake` and surface as
	 * still-unsettled work the heartbeat owns.
	 */
	private async reconcileSubmissions(): Promise<ReadonlyArray<AgentSubmission>> {
		const toStart: Array<AgentSubmission> = [];
		if (!(await this.submissions.hasUnsettledSubmissions())) return toStart;
		try {
			for (const submission of await this.submissions.listUnreadySubmissions()) {
				// A durable abort on an unready row settles here: the row is never
				// claimable, so the attempt-based abort settle can never run — this
				// is the guaranteed escape hatch for every stuck-unready class.
				if (submission.abortRequestedAt !== undefined) {
					await settleUnclaimableSubmission(
						this.submissions,
						submission,
						'aborted',
						new SubmissionAbortedError(),
						this.emitCoordinatorEvent,
					);
					continue;
				}
				const found = this.options.agents.find(
					(record) => record.name === submission.input.agent,
				)?.agent;
				const agent =
					found &&
					submission.input.agent === this.agentName &&
					submission.input.id === this.instance.name
						? found
						: undefined;
				if (!agent) {
					if (
						Date.now() >= unreadySubmissionDeadline(submission, undefined) &&
						(await this.terminalizeUnreadySubmission(
							submission,
							new Error(
								`[flue] Submission target agent "${submission.input.agent}" has no registered definition, so the submission could never start.`,
							),
						))
					) {
						continue;
					}
					console.error('[flue:submission-reconciliation]', {
						agentName: this.agentName,
						instanceId: this.instance.name,
						submissionId: submission.submissionId,
						sessionKey: submission.sessionKey,
						operation: 'materialize_submission',
						outcome: 'agent_unavailable',
					});
					this.emitCoordinatorEvent({
						type: 'submission_recovery',
						submissionId: submission.submissionId,
						kind: submission.kind,
						operation: 'materialize_submission',
						outcome: 'agent_unavailable',
					});
					continue;
				}
				try {
					await this.materializeSubmissionConversation(submission.input, agent);
					await this.submissions.markSubmissionCanonicalReady(submission.submissionId);
				} catch (error) {
					if (
						Date.now() >= unreadySubmissionDeadline(submission, agent) &&
						(await this.terminalizeUnreadySubmission(submission, error))
					) {
						continue;
					}
					this.logSubmissionReconciliationFailure(submission, 'materialize_submission', error);
				}
			}
			for (const settlement of await this.submissions.listPendingSubmissionSettlements()) {
				const submission = await this.submissions.getSubmission(settlement.submissionId);
				if (!submission) continue;
				// Per-item isolation, matching the sibling loops: one bad settlement
				// (e.g. a canonical mismatch) must not skip the running-recovery and
				// runnable-claim passes below for the instance's other work.
				try {
					const writer = await this.ensureConversationWriter();
					await finalizePendingSettlement(
						this.submissions,
						writer,
						settlement,
						this.emitCoordinatorEvent,
					);
				} catch (error) {
					this.logSubmissionReconciliationFailure(submission, 'finalize_settlement', error);
				}
			}
			for (const submission of await this.submissions.listRunningSubmissions()) {
				// A running row whose attempt run the SDK still holds belongs to
				// that run: it is either live in this isolate, or interrupted and
				// due for the SDK's replay, whose handler body reconciles it.
				// Reconciling here too would race that replay for the claim.
				// A running row with NO attempt run is this pass's to recover:
				// the run settled over a hung attempt (its deadline), failed
				// without running (definition missing), or the claim-to-start
				// gap was preempted. When the hung attempt is still live here it
				// is signaled and orphaned first, so its late writes lose the
				// settlement CAS and attempt-id fences and it can no longer
				// append through the shared writer.
				try {
					if (await this.hasLiveAttemptRun(submission.submissionId)) continue;
					const liveController = this.activeControllers.get(submission.submissionId);
					if (liveController) {
						liveController.abort(
							submission.abortRequestedAt !== undefined
								? new SubmissionAbortedError()
								: new SubmissionTimeoutError(),
						);
						console.error('[flue:submission-reconciliation]', {
							agentName: this.agentName,
							instanceId: this.instance.name,
							submissionId: submission.submissionId,
							sessionKey: submission.sessionKey,
							attemptId: submission.attemptId,
							operation: 'enforce_deadline',
							outcome: 'terminated',
							reason:
								submission.abortRequestedAt !== undefined ? 'abort_unhonored' : 'exceeded_timeout',
						});
					}
					const replacement = await this.reconcileInterruptedSubmission(submission);
					// The attempt run starts after the reconcile pass returns —
					// see drive for why starts must escape the pass's tracing
					// activation.
					if (replacement) toStart.push(replacement);
					if (liveController) this.orphanEnforcedAttempt(submission.submissionId, liveController);
				} catch (error) {
					this.logSubmissionReconciliationFailure(submission, 'reconcile_submission', error);
				}
			}
			for (const submission of await this.submissions.listRunnableSubmissions()) {
				// Cloudflare DOs are single-threaded per instance — leases are
				// advisory-only. Set to 0 so reconciliation never misidentifies
				// an active submission as expired. The Node coordinator uses real
				// lease expiry with heartbeat renewal for multi-process safety.
				const claimed = await this.submissions.claimSubmission({
					submissionId: submission.submissionId,
					attemptId: generateAttemptId(),
					ownerId: this.instance.ctx.id.toString(),
					leaseExpiresAt: 0,
				});
				if (claimed) toStart.push(claimed);
			}
		} catch (error) {
			console.error(
				'[flue:submission-reconciliation]',
				{
					agentName: this.agentName,
					instanceId: this.instance.name,
					operation: 'reconcile',
					outcome: 'deferred_to_scheduled_wake',
				},
				error,
			);
			this.emitCoordinatorEvent(
				{
					type: 'submission_recovery',
					operation: 'reconcile_pass',
					outcome: 'deferred',
					error: serializeSubmissionError(error),
				},
				{ errorInfo: classifyError(error) },
			);
		}
		return toStart;
	}

	/**
	 * Auto-fail a queued row whose materialization can never succeed, past its
	 * admission-anchored durability bound (see `unreadySubmissionDeadline`).
	 * Returns whether this coordinator won the terminal transition — a `false`
	 * (another isolate settled first, or a racing claim made the row runnable)
	 * falls back to the deferral logging so nothing is silently dropped.
	 */
	private async terminalizeUnreadySubmission(
		submission: AgentSubmission,
		error: unknown,
	): Promise<boolean> {
		const settled = await settleUnclaimableSubmission(
			this.submissions,
			submission,
			'failed',
			error,
			this.emitCoordinatorEvent,
		);
		if (!settled) return false;
		console.error(
			'[flue:submission-reconciliation]',
			{
				agentName: this.agentName,
				instanceId: this.instance.name,
				submissionId: submission.submissionId,
				sessionKey: submission.sessionKey,
				operation: 'materialize_submission',
				outcome: 'terminated',
			},
			error,
		);
		return true;
	}

	private logSubmissionReconciliationFailure(
		submission: AgentSubmission,
		operation:
			| 'materialize_submission'
			| 'finalize_settlement'
			| 'reconcile_submission'
			| 'start_submission',
		error: unknown,
	): void {
		console.error(
			'[flue:submission-reconciliation]',
			{
				agentName: this.agentName,
				instanceId: this.instance.name,
				submissionId: submission.submissionId,
				sessionKey: submission.sessionKey,
				attemptId: submission.attemptId,
				operation,
				outcome: 'deferred_to_scheduled_wake',
			},
			error,
		);
		this.emitCoordinatorEvent(
			{
				type: 'submission_recovery',
				submissionId: submission.submissionId,
				kind: submission.kind,
				operation,
				outcome: 'deferred',
				attemptCount: submission.attemptCount,
				maxAttempts: submission.maxAttempts,
				error: serializeSubmissionError(error),
			},
			{ errorInfo: classifyError(error) },
		);
	}

	/**
	 * Recover one interrupted attempt. Returns the claimed replacement
	 * submission (if recovery produced one) for the caller's reconcile pass
	 * to start — attempts start only through the drain's guarded path.
	 */
	private async reconcileInterruptedSubmission(
		submission: AgentSubmission,
	): Promise<AgentSubmission | undefined> {
		const conversationWriter = await this.ensureConversationWriter();
		const agent = this.options.agents.find((record) => record.name === this.agentName)?.agent;
		if (!agent) throw new Error('[flue] Agent target unavailable during durable reconciliation.');
		const replacement = await reconcileInterruptedSubmission(
			this.submissions,
			submission,
			agent,
			(submissionId) =>
				this.createDurableContext(submissionSyntheticRequest(submission.input), submissionId),
			{ ownerId: this.instance.ctx.id.toString(), leaseExpiresAt: 0 },
			conversationWriter,
			this.emitCoordinatorEvent,
		);
		return replacement ?? undefined;
	}

	/**
	 * Controllers are keyed by submissionId and shared across attempts, so a
	 * late cleanup from a superseded attempt (its body settling after a
	 * replacement attempt already registered its own controller) must not
	 * delete the replacement's controller — that would sever the abort path
	 * for a live attempt.
	 */
	private deleteControllerIfCurrent(submissionId: string, controller: AbortController): void {
		if (this.activeControllers.get(submissionId) === controller) {
			this.activeControllers.delete(submissionId);
		}
	}

	/**
	 * After the reconcile pass settled over a live-but-hung attempt, orphan
	 * it: drop its controller entry (its own finally-cleanup is unreachable)
	 * and rotate the cached conversation writer so later sessions acquire a
	 * fresh producer — a waking zombie's rejected append then fails only the
	 * stale writer object it holds, never a successor's.
	 */
	private orphanEnforcedAttempt(submissionId: string, controller: AbortController): void {
		this.deleteControllerIfCurrent(submissionId, controller);
		this.conversationWriter = undefined;
		this.conversationWriterCreation = undefined;
	}

	async abortInstance(): Promise<boolean> {
		// One DO instance owns one agent instance; external submissions share one
		// durable session, so a single session-scoped stamp covers the running
		// head and every queued submission behind it.
		const sessionKey = createSessionStorageKey(
			this.agentName,
			this.instance.name,
			SUBMISSION_HARNESS_NAME,
			SUBMISSION_SESSION_NAME,
		);
		const affected = await this.submissions.requestSessionAbort(sessionKey);
		if (affected.length === 0) return false;
		// Abort any of those attempts live in this isolate —
		// processSubmission's catch settles them aborted and the attempt body
		// ensures the next drive. Queued ones settle via the pre-execution
		// abort check once a drive claims them; an evicted running attempt is
		// driven by the durable flag through reconciliation, and a signal-deaf
		// live attempt is settled over by its run deadline (the submission's
		// durability timeout), after which the drive reconciles it aborted.
		for (const submissionId of affected) {
			this.activeControllers.get(submissionId)?.abort(new SubmissionAbortedError());
		}
		await this.ensureDrive();
		return true;
	}

	/**
	 * Admission-side materialization, serialized per instance: ensure the
	 * birth record (find-or-create, no render, no sandbox) and persist the
	 * message's attachments under its conversation id. Idempotent — admission,
	 * replays, and the unready-row recovery pass all run it safely. Returns
	 * the identity for the receipt.
	 */
	private materializeSubmissionConversation(
		input: AgentSubmission['input'],
		agent: Parameters<typeof createAgentSubmissionSessionHandler>[0],
	): Promise<InstanceIdentity> {
		const operation = this.conversationMaterialization.then(async () => {
			const writer = await this.ensureConversationWriter();
			const identity = await ensureInstanceIdentity(writer, agent, input.initialData);
			await materializeSubmissionAttachments(
				input,
				identity.conversationId,
				this.prepared.attachmentStore,
			);
			return identity;
		});
		this.conversationMaterialization = operation.then(
			() => {},
			() => {},
		);
		return operation;
	}

	private async processSubmissionEntry(
		submission: AgentSubmission,
		signal?: AbortSignal,
	): Promise<void> {
		const conversationWriter = await this.ensureConversationWriter();
		await processSubmission({
			submissions: this.submissions,
			submission,
			resolveAgent: (name) => {
				const agent = this.options.agents.find((record) => record.name === name)?.agent;
				if (!agent) throw new Error('[flue] Agent target unavailable during durable processing.');
				return agent;
			},
			createContext: (submissionId) =>
				this.createDurableContext(submissionSyntheticRequest(submission.input), submissionId),
			conversationWriter,
			emitCoordinatorEvent: this.emitCoordinatorEvent,
			signal,
		});
	}

	private async admitAttachedSubmission(
		message: DeliveredMessage,
		options: AttachedAgentSubmissionOptions = {},
	) {
		const { traceCarrier, initialData, uid, idempotencyKey } = options;
		const input = await createDirectAgentSubmissionInput({
			agent: this.agentName,
			id: this.instance.name,
			message,
			initialData,
			traceCarrier,
			...(idempotencyKey !== undefined ? { idempotencyKey } : {}),
		});
		const keyed = idempotencyKey !== undefined;
		const agent = this.options.agents.find((record) => record.name === this.agentName)?.agent;
		if (!agent) throw new Error('[flue] Agent target unavailable during durable admission.');
		const loadReducedState = async () => (await this.ensureConversationWriter()).loadReducedState();
		// A deduplicated replay re-attaches from the stream origin: the original
		// admission-time offset is not persisted, and settlement records are
		// observable from the origin indefinitely.
		const adoptedReceipt = async (submissionId: string) => {
			const reducedUid = (await loadReducedState()).uid;
			if (reducedUid === undefined) return undefined;
			await this.ensureDrive();
			return {
				submissionId,
				offset: '-1',
				uid: reducedUid,
				deduplicated: true as const,
			};
		};
		let contact: InstanceContactAdmission;
		try {
			contact = await admitInstanceContact({
				agent,
				id: this.instance.name,
				initialData,
				uid,
				loadReducedState,
			});
		} catch (error) {
			// Keyed retries can trip their own send condition (a create-only send
			// whose first attempt created the instance): adopt the submission the
			// key already names — the condition was consumed by the original
			// admission. A fresh keyed send keeps today's exact semantics.
			if (keyed && isInstanceContactRejection(error)) {
				const adopted = await adoptKeyedSubmissionReplay(this.submissions, input);
				const receipt = adopted && (await adoptedReceipt(adopted.submissionId));
				if (receipt) return receipt;
			}
			throw error;
		}
		let admitted: AgentSubmission;
		let deduplicated = false;
		try {
			admitted = await this.submissions.admitDirect(input);
		} catch (error) {
			// The store rejects a caller retry byte-exactly (it re-stamps
			// acceptedAt/traceCarrier); a keyed admission converges on identity
			// above the store instead. Adoption only ever swallows the failure
			// when a matching-identity row exists.
			if (!keyed) throw error;
			const adopted = await adoptKeyedSubmissionReplay(this.submissions, input);
			if (!adopted) throw error;
			admitted = adopted;
			deduplicated = true;
		}
		// Live queue signal, emitted immediately after durable admission.
		// At-least-once: admission cannot distinguish an idempotent replay, so
		// replays (keyed dedup included) re-emit.
		this.emitCoordinatorEvent({
			type: 'submission_queued',
			submissionId: admitted.submissionId,
			kind: 'direct',
		});
		// The durable row exists from here on: the drain must be armed even if
		// materialization/readiness/uid below throws, or the queued row would
		// strand with nothing to ever claim it.
		try {
			let identity: InstanceIdentity | undefined;
			if (admitted.canonicalReadyAt === null) {
				identity = await this.materializeSubmissionConversation(input, agent);
				// Tolerate a null return: a concurrent readiness pass may have
				// advanced this row already; null means "already past queued", not
				// a lost submission (rows are never deleted).
				await this.submissions.markSubmissionCanonicalReady(input.submissionId);
			}
			const writer = await this.ensureConversationWriter();
			const offset = deduplicated ? '-1' : writer.offset;
			// An adopted replay may hold neither the contact uid nor a fresh
			// identity (its gate ran before the winning admission materialized) —
			// the birth record is durable by then, so read the identity back.
			let instanceUid = contact.uid ?? identity?.uid;
			if (instanceUid === undefined && deduplicated) {
				instanceUid = (await loadReducedState()).uid;
			}
			if (instanceUid === undefined) {
				throw new Error(
					"[flue] invariant: a materialized instance's birth record must carry a uid.",
				);
			}
			return {
				submissionId: input.submissionId,
				offset,
				uid: instanceUid,
				...(deduplicated ? { deduplicated: true as const } : {}),
			};
		} finally {
			await this.ensureDrive();
		}
	}

	/**
	 * Internal instance lookup for `getAgentInstance()`: existence and uid
	 * from this Durable Object's reduced conversation state. Getting a DO
	 * stub implicitly instantiates the object, so existence is judged by the
	 * birth record, never by DO liveness.
	 */
	private async instanceInfo(): Promise<Response> {
		const reduced = await (await this.ensureConversationWriter()).loadReducedState();
		if (reduced.initialData === undefined) return Response.json({ exists: false });
		return Response.json({
			exists: true,
			...(reduced.uid !== undefined ? { uid: reduced.uid } : {}),
		});
	}

	private async admitDispatch(request: Request): Promise<Response> {
		const input: unknown = await request.json();
		assertAgentDispatchAdmissionInput(input);
		if (input.agent !== this.agentName || input.id !== this.instance.name) {
			return new Response('Invalid internal dispatch target.', { status: 400 });
		}
		const agent = this.options.agents.find((record) => record.name === this.agentName)?.agent;
		if (!agent) return new Response('Dispatch target unavailable.', { status: 404 });
		const keyed = isKeyDerivedSubmissionId(input.submissionId);
		const submissionInput = createDispatchAgentSubmissionInput(input);
		const loadReducedState = async () => (await this.ensureConversationWriter()).loadReducedState();
		try {
			let contact: InstanceContactAdmission;
			try {
				contact = await admitInstanceContact({
					agent,
					id: this.instance.name,
					initialData: input.initialData,
					uid: input.uid,
					loadReducedState,
				});
			} catch (error) {
				// Keyed retries can trip their own send condition (a create-only
				// send whose first attempt created the instance): adopt the
				// submission the key already names — the condition was consumed by
				// the original admission — and echo the recorded uid. A fresh
				// keyed send keeps today's exact semantics.
				if (keyed && isInstanceContactRejection(error)) {
					const adopted = await adoptKeyedSubmissionReplay(this.submissions, submissionInput);
					const adoptedUid = adopted ? (await loadReducedState()).uid : undefined;
					if (adopted && adoptedUid !== undefined) {
						await this.ensureDrive();
						return Response.json({
							submissionId: adopted.submissionId,
							acceptedAt: adopted.input.acceptedAt,
							uid: adoptedUid,
							deduplicated: true,
						});
					}
				}
				throw error;
			}
			const admission = await this.submissions.admitDispatch(input);
			let submission: AgentSubmission;
			let deduplicated = false;
			if (admission.kind === 'submission') {
				submission = admission.submission;
			} else {
				// The store rejects a caller retry byte-exactly (it re-stamps
				// acceptedAt); a keyed conflict converges on submission identity
				// above the store. Everything else — unkeyed conflicts and keyed
				// divergence — is the structured 409 the Worker side rehydrates.
				const adopted = keyed
					? await adoptKeyedSubmissionReplay(this.submissions, submissionInput)
					: undefined;
				if (!adopted) throw new SubmissionConflictError({ submissionId: input.submissionId });
				submission = adopted;
				deduplicated = true;
			}
			// Live queue signal, emitted immediately after durable admission.
			// At-least-once: admission cannot distinguish an idempotent replay,
			// so replays (keyed dedup included) re-emit.
			this.emitCoordinatorEvent({
				type: 'submission_queued',
				submissionId: submission.submissionId,
				kind: 'dispatch',
			});
			// The durable row exists from here on: the drain must be armed even if
			// materialization/readiness/uid below throws, or the queued row would
			// strand with nothing to ever claim it.
			try {
				let identity: InstanceIdentity | undefined;
				if (submission.canonicalReadyAt === null) {
					identity = await this.materializeSubmissionConversation(submissionInput, agent);
					// Tolerate a null return (see the direct path): a concurrent readiness
					// pass may have advanced this row already; null is not a lost submission.
					await this.submissions.markSubmissionCanonicalReady(input.submissionId);
				}
				// The uid rides every receipt: echoed for a continuing send, minted by
				// materialization's identity ensure for a creating one. An adopted
				// replay may hold neither (its gate ran before the winning admission
				// materialized) — read the recorded identity back instead.
				let uid = contact.uid ?? identity?.uid;
				if (uid === undefined && deduplicated) uid = (await loadReducedState()).uid;
				if (uid === undefined) {
					throw new Error(
						"[flue] invariant: a materialized instance's birth record must carry a uid.",
					);
				}
				return Response.json({
					submissionId: submission.submissionId,
					// The stored row's timestamp, so a deduplicated replay echoes the
					// ORIGINAL admission's receipt (identical on a fresh admission).
					acceptedAt: submission.input.acceptedAt,
					uid,
					...(deduplicated ? { deduplicated: true } : {}),
				});
			} finally {
				await this.ensureDrive();
			}
		} catch (error) {
			// Structured body so the dispatch() caller's enqueue can rehydrate the
			// typed admission error (`type` selects the class; `uid` restores the
			// instance-exists 409's incarnation field; `submissionId` restores the
			// submission-conflict 409's existing id) with caller-safe details intact.
			if (
				error instanceof InvalidRequestError ||
				error instanceof AgentInstanceNotFoundError ||
				error instanceof AgentInstanceExistsError ||
				error instanceof SubmissionConflictError
			) {
				return Response.json(
					{
						type: error.type,
						error: error.message,
						details: error.details,
						...(error instanceof AgentInstanceExistsError ? { uid: error.uid } : {}),
						...(error instanceof SubmissionConflictError
							? { submissionId: error.submissionId }
							: {}),
					},
					{ status: error.status },
				);
			}
			throw error;
		}
	}
}

/**
 * Translate the Agents SDK's attempt-wide abort reason into the submission
 * error `processSubmission` keys its settlement on: the run deadline is the
 * submission's durability timeout; anything else (an SDK cancellation) is an
 * abort.
 */
function submissionAbortReason(reason: unknown): Error {
	return isTaskError(reason, 'TaskDeadlineExceededError')
		? new SubmissionTimeoutError()
		: new SubmissionAbortedError();
}

/**
 * Whether an error the Agents SDK reported through `onError` is one it
 * recorded against a Task run without (or over) its handler — the cases
 * that leave a running submission row behind with no run to finish it.
 * Matched by name: `@flue/runtime` does not import `agents`.
 */
function isTaskRecordedFailure(error: unknown): boolean {
	return (
		isTaskError(error, 'TaskDeadlineExceededError') ||
		isTaskError(error, 'TaskAttemptsExhaustedError') ||
		isTaskError(error, 'MissingTaskDefinitionError')
	);
}

function isTaskError(error: unknown, name: string): boolean {
	return (
		typeof error === 'object' &&
		error !== null &&
		'name' in error &&
		(error as { name: unknown }).name === name
	);
}

function isInternalDispatchRequest(request: Request): boolean {
	return (
		request.method === 'POST' &&
		new URL(request.url).pathname === CLOUDFLARE_AGENT_INTERNAL_DISPATCH_PATH
	);
}

function isInternalInstanceInfoRequest(request: Request): boolean {
	return (
		request.method === 'GET' &&
		new URL(request.url).pathname === CLOUDFLARE_AGENT_INTERNAL_INSTANCE_INFO_PATH
	);
}

/**
 * Whether the request is an abort for this agent instance
 * (`POST .../agents/<name>/<id>/abort`). Matched by exact tail position (not a
 * loose substring) so an agent or instance named "abort" cannot misroute.
 */
function isAbortRequest(request: Request, agentName: string, instanceName: string): boolean {
	if (request.method !== 'POST') return false;
	const segments = new URL(request.url).pathname.split('/');
	const n = segments.length;
	if (n < 4) return false;
	return (
		segments[n - 1] === 'abort' &&
		decodeURIComponent(segments[n - 2] as string) === instanceName &&
		decodeURIComponent(segments[n - 3] as string) === agentName
	);
}
