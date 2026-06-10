import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { io as createClient, Socket as ClientSocket } from 'socket.io-client';
import { WebsocketGateway } from './websocket.gateway';

@Injectable()
export class FawssService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(FawssService.name);
  private fawssClient: ClientSocket | null = null;
  private readonly fawssUrl: string;

  constructor(
    private readonly configService: ConfigService,
    private readonly websocketGateway: WebsocketGateway,
  ) {
    const fawssPrivateBaseUrl = this.configService.get<string>('FAWSS_PRIVATE_URL') ?? 'https://pilot-fawss-uds.pi42.com/auth-stream';
    const fawssPrivateListenKey = this.configService.get<string>('FAWSS_PRIVATE_LISTEN_KEY');
    
    this.fawssUrl = fawssPrivateListenKey
      ? `${fawssPrivateBaseUrl.replace(/\/$/, '')}/${fawssPrivateListenKey}`
      : fawssPrivateBaseUrl;
  }

  onModuleInit() {
    this.connectToFawss();
  }

  onModuleDestroy() {
    if (this.fawssClient) {
      this.fawssClient.disconnect();
    }
  }

  private connectToFawss() {
    const fawssPrivateJwt = this.configService.get<string>('FAWSS_PRIVATE_JWT') ?? this.configService.get<string>('PI42_JWT_FAWSS');
    const fawssPrivateListenKey = this.configService.get<string>('FAWSS_PRIVATE_LISTEN_KEY');

    if (!fawssPrivateJwt && !fawssPrivateListenKey) {
      this.logger.warn('[FAWSS] Private upstream DISABLED. Set FAWSS_PRIVATE_JWT or FAWSS_PRIVATE_LISTEN_KEY to enable.');
      return;
    }

    this.fawssClient = createClient(this.fawssUrl, {
      reconnection: true,
      timeout: 10000,
      withCredentials: Boolean(fawssPrivateJwt),
      extraHeaders: fawssPrivateJwt ? { cookie: `jwtFawss=${fawssPrivateJwt}` } : undefined,
    });

    this.fawssClient.on('connect', () => {
      this.logger.log(`[FAWSS] upstream connected → ${this.fawssUrl}`);
    });

    this.fawssClient.onAny((eventName: string, data: unknown) => {
      if (eventName === 'connect' || eventName === 'disconnect') return;

      const accountId = this.getAccountIdFromData(data);
      if (!accountId) {
        this.logger.warn(`[FAWSS] Dropped event '${eventName}' because accountId could not be extracted. Payload: ${JSON.stringify(data).substring(0, 200)}...`);
        return;
      }

      // Relay event via gateway
      this.websocketGateway.emitToAccount(accountId, eventName, data);
    });

    this.fawssClient.on('disconnect', (r: string) => this.logger.log(`[FAWSS] disconnected: ${r}`));
    this.fawssClient.on('connect_error', (e: Error) => this.logger.error(`[FAWSS] connect error: ${e.message}`));
    this.fawssClient.on('error', (e: unknown) => this.logger.error(`[FAWSS] socket error:`, e));
    this.fawssClient.io.on('reconnect_attempt', (n: number) => this.logger.log(`[FAWSS] reconnect attempt #${n}`));
  }

  private getAccountIdFromData(data: unknown): string | null {
    if (!data || typeof data !== 'object') return null;
    const raw = (data as { accountId?: number | string }).accountId;
    const val = String(raw ?? '').trim();
    return val || null;
  }
}
