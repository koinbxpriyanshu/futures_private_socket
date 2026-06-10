import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ConfigService } from '@nestjs/config';
import { AzureIoAdapter } from './websocket/azure-io.adapter';
import * as express from 'express';
import * as path from 'path';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const configService = app.get(ConfigService);

  const port = configService.get<number>('PORT', 3023);
  const connectionString = configService.get<string>('AZURE_CONNECTION_STRING');
  const hub = configService.get<string>('AZURE_PRIVATE_HUB', 'KoinBX_Private_Hub');

  if (!connectionString) {
    throw new Error('AZURE_CONNECTION_STRING is required.');
  }

  // Use the custom Azure IoAdapter for WebSockets
  app.useWebSocketAdapter(new AzureIoAdapter(app, hub, connectionString));

  // Serve static HTML (public/index.html)
  const publicDir = path.join(process.cwd(), 'public');
  app.use(express.static(publicDir));
  
  // Health check
  app.use('/health', (req: any, res: any) => {
    res.json({ status: 'ok' });
  });

  await app.listen(port);

  // Print startup info
  const azureEndpoint = (connectionString.match(/Endpoint=([^;]+)/) ?? [])[1] ?? '';
  const clientUrl = `${azureEndpoint}/clients/socketio/hubs/${hub}`;
  const fawssPrivateBaseUrl = configService.get<string>('FAWSS_PRIVATE_URL') ?? 'https://pilot-fawss-uds.pi42.com/auth-stream';
  const fawssPrivateListenKey = configService.get<string>('FAWSS_PRIVATE_LISTEN_KEY');
  const fawssUrl = fawssPrivateListenKey ? `${fawssPrivateBaseUrl.replace(/\/$/, '')}/${fawssPrivateListenKey}` : fawssPrivateBaseUrl;
  const isFawssEnabled = Boolean(configService.get<string>('FAWSS_PRIVATE_JWT') || fawssPrivateListenKey);

  console.log(`NestJS private websocket server listening on port ${port}`);
  console.log(`Azure web pubsub hub : ${hub}`);
  console.log(`Pi42 private FAWSS   : ${isFawssEnabled ? fawssUrl : 'DISABLED'}`);
  console.log(`DB cache             : warming up on first connection`);
  console.log(``);
  console.log(`┌──────────────────────────────────────────────────────────┐`);
  console.log(`│  CLIENT URL (Azure Web PubSub)                           │`);
  console.log(`│  ${clientUrl.padEnd(56)}│`);
  console.log(`└──────────────────────────────────────────────────────────┘`);
}
bootstrap();
