/**
 * MCP over HTTP
 *
 * Mounts the GitNexus MCP server on Express using StreamableHTTP transport.
 * Each connecting client gets its own stateful session; the LocalBackend
 * is shared across all sessions (thread-safe — lazy LadybugDB per repo).
 *
 * Also mounts classic SSE transport endpoints (GET /api/mcp/sse +
 * POST /api/mcp/message) for clients that expect the older SSE pattern.
 *
 * Sessions are cleaned up on explicit close or after SESSION_TTL_MS of inactivity
 * (guards against network drops that never trigger onclose).
 */

import type { Express, Request, Response } from 'express';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { createMCPServer } from '../mcp/server.js';
import type { LocalBackend } from '../mcp/local/local-backend.js';
import { randomUUID } from 'crypto';
import { logger } from '../core/logger.js';

interface MCPSession {
  server: Server;
  transport: StreamableHTTPServerTransport;
  lastActivity: number;
}

interface SSESession {
  server: Server;
  transport: SSEServerTransport;
  lastActivity: number;
  heartbeat: NodeJS.Timeout | null;
}

/** Idle sessions are evicted after 30 minutes */
const SESSION_TTL_MS = 30 * 60 * 1000;
/** Cleanup sweep runs every 5 minutes */
const CLEANUP_INTERVAL_MS = 5 * 60 * 1000;

export function mountMCPEndpoints(app: Express, backend: LocalBackend): () => Promise<void> {
  const sessions = new Map<string, MCPSession>();
  const sseSessions = new Map<string, SSESession>();

  // Periodic cleanup of idle sessions (guards against network drops)
  const cleanupTimer = setInterval(() => {
    const now = Date.now();
    for (const [id, session] of sessions) {
      if (now - session.lastActivity > SESSION_TTL_MS) {
        try {
          session.server.close();
        } catch {}
        sessions.delete(id);
      }
    }
    for (const [id, session] of sseSessions) {
      if (now - session.lastActivity > SESSION_TTL_MS) {
        clearInterval(session.heartbeat ?? undefined);
        try {
          session.server.close();
        } catch {}
        sseSessions.delete(id);
      }
    }
  }, CLEANUP_INTERVAL_MS);
  cleanupTimer.unref();

  const handleMcpRequest = async (req: Request, res: Response) => {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;

    if (sessionId && sessions.has(sessionId)) {
      // Existing session — delegate to its transport
      const session = sessions.get(sessionId)!;
      session.lastActivity = Date.now();
      await session.transport.handleRequest(req, res, req.body);
    } else if (sessionId) {
      // Unknown/expired session ID — tell client to re-initialize (per MCP spec)
      res.status(404).json({
        jsonrpc: '2.0',
        error: { code: -32001, message: 'Session not found. Re-initialize.' },
        id: null,
      });
    } else if (req.method === 'POST') {
      // No session ID — new client initializing
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
      });
      const server = createMCPServer(backend);
      await server.connect(transport);
      await transport.handleRequest(req, res, req.body);

      if (transport.sessionId) {
        sessions.set(transport.sessionId, { server, transport, lastActivity: Date.now() });
        transport.onclose = () => {
          sessions.delete(transport.sessionId!);
        };
      }
    } else {
      res.status(400).json({
        jsonrpc: '2.0',
        error: { code: -32000, message: 'No valid session. Send a POST to initialize.' },
        id: null,
      });
    }
  };

  // Streamable HTTP endpoint
  app.all('/api/mcp', (req: Request, res: Response) => {
    void handleMcpRequest(req, res).catch((err: any) => {
      logger.error({ err }, 'MCP StreamableHTTP request failed:');
      if (res.headersSent) return;
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32000, message: 'Internal MCP server error' },
        id: null,
      });
    });
  });

  // ── SSE transport ────────────────────────────────────────────────────
  // Classic MCP SSE pattern:
  //   GET  /api/mcp/sse      — establish SSE stream (server → client)
  //   POST /api/mcp/message  — client → server messages

  const HEARTBEAT_INTERVAL_MS = 15_000; // 15s keepalive

  app.get('/api/mcp/sse', (req: Request, res: Response) => {
    const transport = new SSEServerTransport('/api/mcp/message', res);
    const server = createMCPServer(backend);

    server.connect(transport).catch((err: any) => {
      logger.error({ err }, 'SSE MCP server connect failed:');
      if (!res.headersSent) {
        res.status(500).end('Internal MCP server error');
      }
    });

    const heartbeat = setInterval(() => {
      try {
        res.write(': heartbeat\n\n');
      } catch {
        // connection gone — cleanup handles it
      }
    }, HEARTBEAT_INTERVAL_MS);
    heartbeat.unref();

    const teardown = () => {
      clearInterval(heartbeat);
      sseSessions.delete(transport.sessionId);
    };

    const session: SSESession = {
      server,
      transport,
      lastActivity: Date.now(),
      heartbeat,
    };
    sseSessions.set(transport.sessionId, session);

    transport.onclose = teardown;
    req.on('close', teardown);

    logger.info({ sessionId: transport.sessionId }, 'SSE MCP session opened');
  });

  app.post('/api/mcp/message', (req: Request, res: Response) => {
    const sessionId =
      (req.query.sessionId as string) || (req.headers['mcp-session-id'] as string);

    if (!sessionId) {
      res.status(400).json({ error: 'Missing session ID' });
      return;
    }

    const session = sseSessions.get(sessionId);
    if (!session) {
      res.status(404).json({ error: 'Session not found' });
      return;
    }

    session.lastActivity = Date.now();
    session.transport.handlePostMessage(req, res).catch((err: any) => {
      logger.error({ err, sessionId }, 'SSE MCP POST handler failed:');
      if (!res.headersSent) {
        res.status(500).end('Internal error');
      }
    });
  });

  const cleanup = async () => {
    clearInterval(cleanupTimer);

    const streamableClosers = [...sessions.values()].map(async (session) => {
      try {
        await Promise.resolve(session.server.close());
      } catch {}
    });
    sessions.clear();

    const sseClosers = [...sseSessions.values()].map(async (session) => {
      clearInterval(session.heartbeat!);
      try {
        await Promise.resolve(session.server.close());
      } catch {}
    });
    sseSessions.clear();

    await Promise.allSettled([...streamableClosers, ...sseClosers]);
  };

  console.log('MCP endpoints: /api/mcp (StreamableHTTP), /api/mcp/sse + /api/mcp/message (SSE)');
  return cleanup;
}
