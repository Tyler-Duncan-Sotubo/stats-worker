import { Module, Global } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import * as schema from './schema';

export const DRIZZLE = Symbol('DRIZZLE');
export type DrizzleDB = ReturnType<typeof drizzle<typeof schema>>;

@Global()
@Module({
  providers: [
    {
      provide: DRIZZLE,
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const pool = new Pool({
          connectionString: config.getOrThrow('DATABASE_URL'),
          max: 5,
          idleTimeoutMillis: 30000,
          connectionTimeoutMillis: 15000,
          keepAlive: true,
          keepAliveInitialDelayMillis: 10000,
        });

        // Handle stale connection errors gracefully
        // instead of crashing the process
        pool.on('error', (err) => {
          console.error(
            'Pool client error — removing stale connection:',
            err.message,
          );
        });

        return drizzle(pool, { schema });
      },
    },
  ],
  exports: [DRIZZLE],
})
export class DatabaseModule {}
