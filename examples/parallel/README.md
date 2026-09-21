# Parallel web research

An agent using Parallel's hosted Search MCP through Flue's native MCP hooks. No Parallel SDK, sandbox, or OAuth flow is needed.

## Setup

From the repository root:

```sh
pnpm install
pnpm --filter 'example-parallel^...' build
cd examples/parallel
cp .env.example .env
```

Set `ANTHROPIC_API_KEY` in `.env` for the example model. `PARALLEL_API_KEY` is optional: leave it blank for anonymous light use, or supply your own key for authenticated access. See [Parallel's current limits and authentication guidance](https://docs.parallel.ai/integrations/mcp/search-mcp). Model inference costs are separate. Never commit either key.

The example already contains the connection and agent: there is no need to run `flue add` or start a blueprint registry. When adding Parallel to a different project, `flue add tooling parallel --print` prints a guide for a coding agent or human to apply; it does not install files or run the integration.

A `parallel-cli` login or MCP server configured in your coding assistant does not automatically authenticate this example. Provide `PARALLEL_API_KEY` in its environment for authenticated access. Keep local CLI credential-file readers out of shared or deployed agent code.

### Using OpenAI instead

In `src/agents/research.ts`, replace the `useModel` line with:

```ts
useModel('openai/gpt-5.6-luna');
```

Set `OPENAI_API_KEY` in `.env` instead of `ANTHROPIC_API_KEY`. Your OpenAI API account must have access to this model. The Parallel connection is unchanged; there is no automatic provider fallback. For an existing application, retain its model unless you intend to change it.

## Node

```sh
pnpm dev
```

Or run the agent directly:

```sh
pnpm exec flue run src/agents/research.ts --message 'Search for Flue MCP documentation, fetch the official MCP guide, and summarize it with source links.'
```

Build with `pnpm build`. Supply real environment variables when running the production artifact (`node dist/server.mjs`); it does not load `.env`.

### Standalone Node script

For a minimal test without Vite or an HTTP server, create `run-research.ts` in this example directory:

```ts
import { randomUUID } from 'node:crypto';
import { init } from '@flue/runtime';
import { start } from '@flue/runtime/node';
import { Research } from './src/agents/research.ts';

const flue = await start({ agents: [Research] });
try {
  const agent = init(Research, { id: randomUUID() });
  const receipt = await agent.dispatch(
    'What does Flue do, and who maintains it? Search the web and cite sources.',
  );
  console.log((await agent.read(receipt)).text);
} finally {
  await flue.stop();
}
```

Run it with Node 22.19+ after the setup steps above:

```sh
node --env-file=.env --experimental-strip-types run-research.ts
```

Unlike `flue run`, a plain Node script does not automatically load `.env`; the flag is intentional. This uses in-memory conversations by default, so nothing survives process exit. Flue may also expose its built-in `task` tool; this agent declares no delegates. The Parallel connection's allowlist restricts the MCP tools, not unrelated framework tools.

## Cloudflare

```sh
cp .env.example .dev.vars
# Set credentials in .dev.vars, then:
pnpm dev:cloudflare
```

This runs the same agent in workerd. `vite.cloudflare.config.ts` installs the Flue plugin before the Cloudflare plugin; `wrangler.jsonc` declares the generated `FlueResearchAgent` migration. The compatibility date and `nodejs_compat` allow server-side `process.env` access. Build with `pnpm build:cloudflare`; production credentials belong in Worker secrets, not the bundle.

Both servers mount `/agents/research`. Use the [Flue client](https://flueframework.com/docs/sdk/overview/) or demo UI to send the prompt above. Confirm both `mcp__parallel__web_search` and `mcp__parallel__web_fetch` appear in the conversation, with source URLs in the answer. The example has no route authentication: add access controls before public deployment.

## Tests

```sh
pnpm test
pnpm check:types
```

Offline tests exercise the real Flue MCP client with mocked HTTP, covering credential normalization, tool filtering, and error propagation. They require no provider credentials. Live verification should run on both targets with and without a Parallel key; it consumes service usage and is not part of ordinary CI.

The connection deliberately remains required. Failed discovery stops the submission; invalid configured credentials are not retried anonymously. Do not send sensitive queries or URLs without authorization, and treat fetched text as untrusted content.
