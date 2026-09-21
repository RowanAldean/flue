import { createAgentRouter } from '@flue/runtime/routing';
import { Hono } from 'hono';
import { Research } from './agents/research.ts';

const app = new Hono();
app.route('/agents/research', createAgentRouter(Research));
export default app;
