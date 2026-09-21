import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createMcpConnection } from '@flue/runtime';
import { parallelConnection } from '../src/connections/parallel.ts';

for (const key of [undefined, '', '  ']) {
	test(`omits authorization for ${JSON.stringify(key)}`, () => {
		assert.equal(Object.hasOwn(parallelConnection(key), 'auth'), false);
	});
}

test('trims a supplied credential without replacing it', () => {
	assert.equal(parallelConnection('  test-key  ').auth, 'test-key');
});

test('propagates transport timeouts rather than reporting a connected server', async () => {
	await assert.rejects(
		createMcpConnection({
			...parallelConnection(),
			fetch: async () => {
				throw new DOMException('Test transport timeout', 'TimeoutError');
			},
		}),
		/Test transport timeout/,
	);
});

// Keep the real Flue/MCP client; replace only external HTTP traffic.
for (const key of [undefined, 'test-key']) {
	test(`discovers only search and fetch with ${key ? 'bearer' : 'anonymous'} auth`, async () => {
		const authorization: Array<string | null> = [];
		const connection = await createMcpConnection({
			...parallelConnection(key),
			fetch: async (input, init) => {
				const request = new Request(input, init);
				authorization.push(request.headers.get('authorization'));
				const message = (await request.json()) as { id?: number; method: string };
				if (message.id === undefined) return new Response(null, { status: 202 });
				const result =
					message.method === 'initialize'
						? {
								protocolVersion: '2025-03-26',
								capabilities: { tools: {} },
								serverInfo: { name: 'fixture', version: '1' },
							}
						: {
								tools: ['web_search', 'web_fetch', 'unrelated_tool'].map((name) => ({
									name,
									inputSchema: { type: 'object', properties: {} },
								})),
							};
				return Response.json({ jsonrpc: '2.0', id: message.id, result });
			},
		});
		try {
			assert.deepEqual(
				connection.tools.map((tool) => tool.name),
				['mcp__parallel__web_search', 'mcp__parallel__web_fetch'],
			);
			assert.ok(authorization.length >= 2);
			assert.ok(authorization.every((header) => header === (key ? 'Bearer test-key' : null)));
		} finally {
			await connection.close();
		}
	});
}

for (const status of [401, 429, 503]) {
	test(`surfaces HTTP ${status} without retrying anonymously`, async () => {
		const headers: Array<string | null> = [];
		await assert.rejects(
			createMcpConnection({
				...parallelConnection('invalid-test-key'),
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (request.url === 'https://search.parallel.ai/mcp')
						headers.push(request.headers.get('authorization'));
					return new Response('Test failure', { status });
				},
			}),
		);
		assert.ok(headers.length > 0);
		assert.ok(headers.every((header) => header === 'Bearer invalid-test-key'));
	});
}
