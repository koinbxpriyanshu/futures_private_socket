import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as jwt from 'jsonwebtoken';

export interface KoinBxJwtPayload {
  PublicGUID: string;
  sessionId: string;
  [key: string]: unknown;
}

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);
  private readonly jwtSecret: string | undefined;

  constructor(private readonly configService: ConfigService) {
    this.jwtSecret = this.configService.get<string>('JWT_SECRET') ?? this.configService.get<string>('SECRET');
    if (!this.jwtSecret) {
      this.logger.warn('No JWT_SECRET set - tokens will be decoded WITHOUT signature verification (dev mode)');
    }
  }

  public verifyUserJwt(token: string): KoinBxJwtPayload | null {
    try {
      if (this.jwtSecret) {
        return jwt.verify(token, this.jwtSecret, { algorithms: ['HS256', 'HS384', 'HS512'] }) as KoinBxJwtPayload;
      }
      // Dev-mode: decode without verification
      return jwt.decode(token) as KoinBxJwtPayload | null;
    } catch (err: any) {
      this.logger.error(`Verification failed: ${err.message}`);
      return null;
    }
  }
}
