import { IoAdapter } from '@nestjs/platform-socket.io';
import { INestApplicationContext } from '@nestjs/common';
import { useAzureSocketIO } from '@azure/web-pubsub-socket.io';

export class AzureIoAdapter extends IoAdapter {
  constructor(
    private app: INestApplicationContext,
    private readonly hub: string,
    private readonly connectionString: string,
  ) {
    super(app);
  }

  createIOServer(port: number, options?: any): any {
    options = {
      ...options,
      cors: { origin: '*' },
    };
    
    // Create the standard Socket.IO server via NestJS's default adapter
    const server = super.createIOServer(port, options);
    
    // Inject Azure Web PubSub Socket.IO middleware
    useAzureSocketIO(server, {
      hub: this.hub,
      connectionString: this.connectionString,
    });

    return server;
  }
}
