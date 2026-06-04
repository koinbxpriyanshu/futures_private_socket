import { Module } from '@nestjs/common';
import { WebsocketGateway } from './websocket.gateway';
import { FawssService } from './fawss.service';

@Module({
  providers: [WebsocketGateway, FawssService],
})
export class WebsocketModule {}
