import { Injectable, Logger } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';

@Injectable()
export class CacheService {
  private readonly logger = new Logger(CacheService.name);

  constructor(@InjectRedis() private readonly redis: Redis) {}

  // ── Core get/set ──────────────────────────────────────────────────────

  async get<T>(key: string): Promise<T | null> {
    const hit = await this.redis.get(key);
    return hit ? (JSON.parse(hit) as T) : null;
  }

  async set<T>(key: string, value: T, ttlSeconds: number): Promise<void> {
    await this.redis.set(key, JSON.stringify(value), 'EX', ttlSeconds);
  }

  async del(key: string): Promise<void> {
    await this.redis.del(key);
  }

  // ── Cache-aside — the main pattern used everywhere ────────────────────

  async cached<T>(
    key: string,
    ttlSeconds: number,
    fetcher: () => Promise<T>,
  ): Promise<T> {
    const hit = await this.get<T>(key);
    if (hit !== null) return hit;

    const result = await fetcher();
    await this.set(key, result, ttlSeconds);
    return result;
  }

  // ── Pattern-based invalidation ────────────────────────────────────────
  // Use namespaced patterns — e.g. 'awards:artist:123*'

  async invalidatePattern(pattern: string): Promise<number> {
    const keys = await this.redis.keys(pattern);
    if (!keys.length) return 0;

    await this.redis.del(...keys);
    this.logger.log(`Invalidated ${keys.length} keys matching "${pattern}"`);
    return keys.length;
  }

  // Invalidate multiple patterns in one call
  async invalidatePatterns(patterns: string[]): Promise<number> {
    const counts = await Promise.all(
      patterns.map((p) => this.invalidatePattern(p)),
    );
    return counts.reduce((sum, n) => sum + n, 0);
  }

  // ── TTL presets — use these across all services ───────────────────────

  static readonly TTL = {
    SHORT: 60 * 5, //  5 minutes  — live counters, trending
    MEDIUM: 60 * 60 * 6, //  6 hours    — rankings, top lists
    LONG: 60 * 60 * 24, // 24 hours    — artist profiles, song pages
    EXTENDED: 60 * 60 * 24 * 7, // 7 days   — certifications, awards (rarely change)
  } as const;
}
