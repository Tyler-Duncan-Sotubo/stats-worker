import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import * as cheerio from 'cheerio';

export interface RiaaCertification {
  artist: string;
  title: string;
  level: string; // 'gold' | 'platinum' | 'diamond'
  units: number | null; // 16 for 16x Platinum, null for Gold/Diamond
  certifiedAt: string; // 'October 30, 2023'
  label: string;
  format: string; // 'SINGLE' | 'ALBUM'
  riaaId: string; // e.g. '424111' — from the row id
}

@Injectable()
export class RiaaCertificationService {
  private readonly logger = new Logger(RiaaCertificationService.name);
  private readonly baseUrl = 'https://www.riaa.com/gold-platinum';

  async searchArtist(artistName: string): Promise<RiaaCertification[]> {
    const url = `${this.baseUrl}/?tab_active=default-award&ar=${encodeURIComponent(artistName)}`;

    const { data } = await axios.get<string>(url, {
      timeout: 15_000,
      headers: {
        'User-Agent': 'tooXclusiveStatsBot/1.0 (+https://tooxclusive.com)',
        Accept: 'text/html,application/xhtml+xml',
      },
    });

    const $ = cheerio.load(data);
    const results: RiaaCertification[] = [];

    $('tr.table_award_row').each((_, row) => {
      const $row = $(row);

      // row id = "default_447989" → riaaId = "447989"
      const riaaId = ($row.attr('id') ?? '').replace('default_', '');

      // level comes from data-share-desc:
      // "earned RIAA 16x Platinum Award for GOD'S PLAN"
      // "earned RIAA Diamond Award for NICE FOR WHAT"
      // "earned RIAA Gold Award for RAINING IN HOUSTON"
      const shareDesc = $row.find('.share_text').attr('data-share-desc') ?? '';
      const { level, units } = this.parseLevelFromDesc(shareDesc);

      const artist = $row.find('td.artists_cell').text().trim();
      const title = $row.find('td.others_cell').eq(0).text().trim();
      const certifiedAt = $row.find('td.others_cell').eq(1).text().trim();
      const label = $row.find('td.others_cell').eq(2).text().trim();
      const format = $row
        .find('td.format_cell')
        .text()
        .replace('MORE DETAILS', '')
        .trim();

      if (!artist || !title || !level) return;

      results.push({
        artist,
        title,
        level,
        units,
        certifiedAt,
        label,
        format,
        riaaId,
      });
    });

    return results;
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  // "earned RIAA 16x Platinum Award for GOD'S PLAN" → { level: 'platinum', units: 16 }
  // "earned RIAA Diamond Award for NICE FOR WHAT"   → { level: 'diamond',  units: null }
  // "earned RIAA Gold Award for RAINING IN HOUSTON" → { level: 'gold',     units: null }

  private parseLevelFromDesc(desc: string): {
    level: string;
    units: number | null;
  } {
    const lower = desc.toLowerCase();

    if (lower.includes('diamond')) {
      return { level: 'diamond', units: null };
    }

    if (lower.includes('gold')) {
      return { level: 'gold', units: null };
    }

    if (lower.includes('platinum')) {
      // "16x platinum" → 16, "1x platinum" → 1
      const match = desc.match(/(\d+)x\s+Platinum/i);
      const units = match ? parseInt(match[1], 10) : 1;
      return { level: 'platinum', units };
    }

    return { level: '', units: null };
  }

  private parseDate(raw: string): string | null {
    if (!raw) return null;
    const d = new Date(raw);
    return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
  }
}
