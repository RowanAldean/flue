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

## Node

```sh
pnpm dev
```

Or run the agent directly:

```sh
pnpm exec flue run src/agents/research.ts --message 'Search for Flue MCP documentation, fetch the official MCP guide, and summarize it with source links.'
```

Build with `pnpm build`. Supply real environment variables when running the production artifact (`node dist/server.mjs`); it does not load `.env`.

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
