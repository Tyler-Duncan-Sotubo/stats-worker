import { Global, Module } from '@nestjs/common';
import { CacheService } from './cache.service';
import { CacheWarmerService } from './cache-warmer.service';

@Global()
@Module({
  providers: [CacheService, CacheWarmerService],
  exports: [CacheService, CacheWarmerService],
})
export class CacheModule {}
