import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as sql from 'mssql';

@Injectable()
export class DatabaseService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DatabaseService.name);
  private pool: sql.ConnectionPool | null = null;
  
  // In-memory cache { userPublicGuid -> pi42AccountId }
  private readonly accountIdCache = new Map<string, string>();

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit() {
    await this.connect();
  }

  async onModuleDestroy() {
    if (this.pool) {
      await this.pool.close();
      this.logger.log('MSSQL Connection pool closed');
    }
  }

  private async connect() {
    const dbConfig: sql.config = {
      server: this.configService.get<string>('PRIMARY_DATABASE_HOST')!,
      port: Number(this.configService.get<number>('PRIMARY_DATABASE_PORT', 1433)),
      user: this.configService.get<string>('PRIMARY_DATABASE_USERNAME')!,
      password: this.configService.get<string>('PRIMARY_DATABASE_PASSWORD')!,
      database: this.configService.get<string>('PRIMARY_DATABASE_NAME')!,
      options: {
        trustServerCertificate: true,
        enableArithAbort: true,
        useUTC: true,
      },
      requestTimeout: 60_000,
      connectionTimeout: 60_000,
      pool: {
        max: 30,
        min: 5,
        idleTimeoutMillis: 30_000,
      },
    };

    try {
      this.pool = await sql.connect(dbConfig);
      this.logger.log('Connected to MSSQL Database');
    } catch (error: any) {
      this.logger.error(`Failed to connect to MSSQL Database: ${error.message}`);
      throw error;
    }
  }

  public getPool(): sql.ConnectionPool {
    if (!this.pool || !this.pool.connected) {
      throw new Error('MSSQL Pool is not connected');
    }
    return this.pool;
  }

  public getCacheSize(): number {
    return this.accountIdCache.size;
  }

  /**
   * DB lookup: UserPublicGUID -> PI42_AccountId from dbo.PI42_Users
   */
  public async lookupPi42AccountId(userPublicGuid: string): Promise<string | null> {
    // 1. Serve from cache if available
    if (this.accountIdCache.has(userPublicGuid)) {
      return this.accountIdCache.get(userPublicGuid)!;
    }

    try {
      const pool = this.getPool();
      const result = await pool
        .request()
        .input('guid', sql.NVarChar(500), userPublicGuid)
        .query<{ PI42_AccountId: number }>(`
          SELECT TOP 1 PI42_AccountId
          FROM dbo.PI42_Users
          WHERE UserPublicGUID = @guid
            AND Row_Status = 1
        `);

      const row = result.recordset[0];
      if (!row) {
        this.logger.warn(`No PI42_Users row found for UserPublicGUID=${userPublicGuid}`);
        return null;
      }

      const accountId = String(row.PI42_AccountId);
      // 2. Store in cache for all future connections from this user
      this.accountIdCache.set(userPublicGuid, accountId);
      
      return accountId;
    } catch (err: any) {
      this.logger.error(`Error looking up PI42_AccountId: ${err.message}`);
      return null;
    }
  }
}
