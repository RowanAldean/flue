// flue-blueprint: tooling/parallel@1
import { defineMcpConnection } from '@flue/runtime';

export function parallelConnection(apiKey?: string) {
	const token = apiKey?.trim();
	return defineMcpConnection({
		name: 'parallel',
		url: 'https://search.parallel.ai/mcp',
		transport: 'streamable-http',
		tools: ['web_search', 'web_fetch'],
		...(token ? { auth: token } : {}),
	});
}
