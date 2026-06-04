import {
  WebSocketGateway,
  WebSocketServer,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import { Server, Socket } from 'socket.io';
import { AuthService } from '../auth/auth.service';
import { DatabaseService } from '../database/database.service';

@WebSocketGateway()
export class WebsocketGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  server!: Server;

  private readonly logger = new Logger(WebsocketGateway.name);
  private readonly privateSocketAccounts = new Map<string, Set<string>>();

  constructor(
    private readonly authService: AuthService,
    private readonly databaseService: DatabaseService,
  ) {}

  async handleConnection(socket: Socket) {
    try {
      const token: string | undefined =
        socket.handshake?.auth?.token ??
        socket.handshake?.query?.token;

      if (!token) {
        throw new Error('unauthorized: no token provided');
      }

      // 1. Verify JWT
      const payload = this.authService.verifyUserJwt(token);
      if (!payload) {
        throw new Error('unauthorized: invalid or expired token');
      }

      const userPublicGuid = String(payload.PublicGUID ?? '').trim();
      if (!userPublicGuid) {
        throw new Error('unauthorized: token missing PublicGUID');
      }

      // 2. Resolve PI42_AccountId
      const pi42AccountId = await this.databaseService.lookupPi42AccountId(userPublicGuid);
      if (!pi42AccountId) {
        throw new Error('unauthorized: user not found in PI42_Users');
      }

      // 3. Attach data to socket
      socket.data.userPublicGuid = userPublicGuid;
      socket.data.accountId = pi42AccountId;

      // 4. Auto-subscribe
      const room = this.roomForAccount(pi42AccountId);
      this.privateSocketAccounts.set(socket.id, new Set([pi42AccountId]));
      socket.join(room);

      // Confirm subscription without exposing accountId
      socket.emit('subscribed', { success: true });
      this.logger.log(`[${socket.id}] Connected and subscribed accountId=${pi42AccountId}`);

    } catch (error: any) {
      this.logger.error(`[${socket.id}] Connection failed: ${error.message}`);
      socket.disconnect(true);
    }
  }

  handleDisconnect(socket: Socket) {
    this.cleanupSocket(socket);
    this.logger.log(`[${socket.id}] Disconnected`);
  }

  @SubscribeMessage('ping')
  handlePing(socket: Socket) {
    return { event: 'pong' }; // NestJS translates this to emitting 'pong' back to client
  }

  // Helper method to emit events to specific accounts (called by FawssService)
  public emitToAccount(accountId: string, eventName: string, data: any) {
    const room = this.roomForAccount(accountId);
    this.server.to(room).emit(eventName, data);
  }

  private roomForAccount(accountId: string): string {
    return `account:${accountId}`;
  }

  private cleanupSocket(socket: Socket) {
    const ownedAccounts = this.privateSocketAccounts.get(socket.id);
    if (ownedAccounts) {
      for (const accountId of ownedAccounts) {
        socket.leave(this.roomForAccount(accountId));
      }
    }
    this.privateSocketAccounts.delete(socket.id);
  }
}
