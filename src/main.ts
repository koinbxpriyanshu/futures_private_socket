import 'dotenv/config';
import fs from 'fs';
import http from 'http';
import path from 'path';
import { Server } from 'socket.io';
import { io as createClient, Socket as ClientSocket } from 'socket.io-client';
import { useAzureSocketIO } from '@azure/web-pubsub-socket.io';

// ─────────────────────────────────────────────────────────────────────────────
// Architecture
//
//  Client  ──►  Azure Web PubSub (KoinBX_Private_Hub)
//                       │
//                       ▼ (tunnel / reverse proxy)
//          This server (Socket.IO on KoinBX_Private_Hub)
//                       │
//                       ▼ (socket.io-client upstream)
//          Pi42 private FAWSS  (https://pilot-fawss-uds.pi42.com/auth-stream)
//
// The client never touches pi42 directly – it only talks to Azure.
// This server is the bridge: it subscribes to pi42 on behalf of each account
// and relays private events to the correct Azure room.
// ─────────────────────────────────────────────────────────────────────────────

const connectionString    = process.env.AZURE_CONNECTION_STRING;
const hub                 = process.env.AZURE_PRIVATE_HUB ?? 'KoinBX_Private_Hub';
const port                = Number(process.env.PORT ?? 3023);
const htmlPath            = path.join(process.cwd(), 'public', 'index.html');
const fawssPrivateBaseUrl = process.env.FAWSS_PRIVATE_URL ?? 'https://pilot-fawss-uds.pi42.com/auth-stream';
const fawssPrivateJwt     = process.env.FAWSS_PRIVATE_JWT ?? process.env.PI42_JWT_FAWSS;
const fawssPrivateListenKey = process.env.FAWSS_PRIVATE_LISTEN_KEY;
const clientToken         = process.env.PRIVATE_SOCKET_CLIENT_TOKEN;

type PrivateSubscribePayload = {
  accountId?: number | string;
  token?: string;
};

if (!connectionString) {
  throw new Error('AZURE_CONNECTION_STRING is required.');
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP server
// ─────────────────────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const requestUrl = req.url ?? '/';
  const pathname   = new URL(requestUrl, `http://localhost:${port}`).pathname;

  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'private-websocket' }));
    return;
  }

  if (pathname === '/' && fs.existsSync(htmlPath)) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(fs.readFileSync(htmlPath, 'utf8'));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: true, service: 'private-websocket' }));
});

// ─────────────────────────────────────────────────────────────────────────────
// Socket.IO – single Server, single useAzureSocketIO call (KoinBX_Private_Hub)
// ─────────────────────────────────────────────────────────────────────────────
const io = new Server(server, {
  cors: { origin: '*' },
});

// ─────────────────────────────────────────────────────────────────────────────
// State
// ─────────────────────────────────────────────────────────────────────────────
const privateSocketAccounts = new Map<string, Set<string>>();

function roomForAccount(accountId: string): string {
  return `account:${accountId}`;
}

function normalizeAccountId(accountId: number | string | undefined): string | null {
  const value = String(accountId ?? '').trim();
  return value || null;
}

function getAccountId(data: unknown): string | null {
  if (!data || typeof data !== 'object') return null;
  return normalizeAccountId((data as { accountId?: number | string }).accountId);
}

// ─────────────────────────────────────────────────────────────────────────────
// Namespace middleware – optional token auth
// ─────────────────────────────────────────────────────────────────────────────
if (clientToken) {
  io.use((socket: any, next: any) => {
    const token = socket.handshake?.auth?.token ?? socket.handshake?.query?.token;
    if (token !== clientToken) {
      return next(new Error('unauthorized'));
    }
    next();
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Subscribe helpers
// ─────────────────────────────────────────────────────────────────────────────
function subscribePrivateAccount(socket: any, payload: PrivateSubscribePayload) {
  const accountId = normalizeAccountId(payload.accountId);
  if (!accountId) {
    socket.emit('privateSubscriptionStatus', { action: 'subscribe', error: 'accountId is required.' });
    return;
  }

  const ownedAccounts = privateSocketAccounts.get(socket.id);
  if (!ownedAccounts) return;

  if (!ownedAccounts.has(accountId)) {
    ownedAccounts.add(accountId);
    socket.join(roomForAccount(accountId));
  }

  socket.emit('privateSubscriptionStatus', { action: 'subscribe', subscribed: [accountId] });
  // console.log(`[${socket.id}] subscribed to account ${accountId}`);
}

function unsubscribePrivateAccount(socket: any, payload: PrivateSubscribePayload) {
  const accountId = normalizeAccountId(payload.accountId);
  if (!accountId) {
    socket.emit('privateSubscriptionStatus', { action: 'unsubscribe', error: 'accountId is required.' });
    return;
  }

  const ownedAccounts = privateSocketAccounts.get(socket.id);
  if (!ownedAccounts || !ownedAccounts.has(accountId)) {
    socket.emit('privateSubscriptionStatus', { action: 'unsubscribe', notSubscribed: [accountId] });
    return;
  }

  ownedAccounts.delete(accountId);
  socket.leave(roomForAccount(accountId));
  socket.emit('privateSubscriptionStatus', { action: 'unsubscribe', unsubscribed: [accountId] });
  // console.log(`[${socket.id}] unsubscribed from account ${accountId}`);
}

function cleanupSocket(socket: any) {
  const ownedAccounts = privateSocketAccounts.get(socket.id);
  if (ownedAccounts) {
    for (const accountId of ownedAccounts) socket.leave(roomForAccount(accountId));
  }
  privateSocketAccounts.delete(socket.id);
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection handler
// ─────────────────────────────────────────────────────────────────────────────
io.on('connection', (socket: any) => {
  // console.log(`[${socket.id}] client connected`);
  privateSocketAccounts.set(socket.id, new Set());

  socket.on('subscribePrivate',   (payload: PrivateSubscribePayload) => subscribePrivateAccount(socket, payload));
  socket.on('unsubscribePrivate', (payload: PrivateSubscribePayload) => unsubscribePrivateAccount(socket, payload));
  socket.on('ping', () => {
    socket.emit('pong');
  });
  socket.on('error',      (err: unknown)    => console.error(`[${socket.id}] socket error:`, err));
  socket.on('disconnect', (reason: string)  => {
    cleanupSocket(socket);
    // console.log(`[${socket.id}] disconnected (${reason})`);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Pi42 private FAWSS upstream
//
// This server connects to pi42's private socket as a CLIENT and relays
// incoming private events to the appropriate Azure room (by accountId).
// ─────────────────────────────────────────────────────────────────────────────
const fawssPrivateUrl = fawssPrivateListenKey
  ? `${fawssPrivateBaseUrl.replace(/\/$/, '')}/${fawssPrivateListenKey}`
  : fawssPrivateBaseUrl;

const fawssPrivateClient: ClientSocket | null =
  fawssPrivateJwt || fawssPrivateListenKey
    ? createClient(fawssPrivateUrl, {
        reconnection: true,
        timeout: 10000,
        withCredentials: Boolean(fawssPrivateJwt),
        extraHeaders: fawssPrivateJwt ? { cookie: `jwtFawss=${fawssPrivateJwt}` } : undefined,
      })
    : null;

if (!clientToken) {
  console.warn('PRIVATE_SOCKET_CLIENT_TOKEN is not set – connections are not token-protected.');
}

if (!fawssPrivateClient) {
  console.warn(
    'Pi42 private FAWSS upstream is DISABLED. ' +
    'Set FAWSS_PRIVATE_JWT, PI42_JWT_FAWSS, or FAWSS_PRIVATE_LISTEN_KEY to enable it.',
  );
}

if (fawssPrivateClient) {
  fawssPrivateClient.on('connect', () => {
    console.log(`upstream connected → pi42 private FAWSS: ${fawssPrivateUrl}`);
  });

  // Relay every private event to the matching Azure room by accountId
  fawssPrivateClient.onAny((eventName: string, data: unknown) => {
    if (eventName === 'connect' || eventName === 'disconnect') return;

    const accountId = getAccountId(data);
    if (!accountId) {
      console.log(`pi42 private event "${eventName}" has no accountId – skipped`);
      return;
    }

    // Emit only the raw event
    io.to(roomForAccount(accountId)).emit(eventName, data);

    // console.log(`[pi42→azure] event="${eventName}" account=${accountId} data=${JSON.stringify(data)}`);
  });

  fawssPrivateClient.on('disconnect',    (reason: string) => console.log(`pi42 private FAWSS disconnected: ${reason}`));
  fawssPrivateClient.on('connect_error', (err: Error)     => console.error(`pi42 private FAWSS connect error: ${err.message}`));
  fawssPrivateClient.on('error',         (err: unknown)   => console.error('pi42 private FAWSS socket error:', err));
  fawssPrivateClient.io.on('reconnect_attempt', (n: number) => console.log(`pi42 private FAWSS reconnect attempt #${n}`));
}

// ─────────────────────────────────────────────────────────────────────────────
// Bootstrap
// ─────────────────────────────────────────────────────────────────────────────
(async () => {
  await useAzureSocketIO(io as any, {
    hub,
    connectionString: connectionString!,
  });

  server.listen(port, () => {
    // Parse Azure endpoint from connection string for client URL
    const azureEndpoint = (connectionString!.match(/Endpoint=([^;]+)/) ?? [])[1] ?? 'your-azure-endpoint';
    const clientUrl     = `${azureEndpoint}/clients/socketio/hubs/${hub}`;

    console.log(`private websocket server listening on port ${port}`);
    console.log(`azure web pubsub hub: ${hub}`);
    console.log(`pi42 private FAWSS:   ${fawssPrivateClient ? fawssPrivateUrl : 'DISABLED'}`);
    console.log(``);
    console.log(`┌──────────────────────────────────────────────────────────┐`);
    console.log(`│  CLIENT CONNECTION URL (use this in your frontend)       │`);
    console.log(`│  ${clientUrl.padEnd(56)}│`);
    console.log(`└──────────────────────────────────────────────────────────┘`);
    console.log(`  Socket path: /clients/socketio/hubs/${hub}`);
  });
})().catch((err) => {
  console.error('Failed to initialize:', err);
  process.exit(1);
});
