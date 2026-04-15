export interface KworbWeeklyRow {
  rank: number;
  trend: string;
  artist: string;
  weeksOnChart: number; // weekly streams
  trackUrl?: string | null; // kworb track page
  artistUrl?: string | null; // kworb artist page
}

export interface KworbWeeklyPayload {
  country: string; // 'ng'
  label: string; // e.g. 'Week of Oct 18, 2025' (if you derive it)
  fetchedAtISO: string;
  rows: KworbWeeklyRow[];
}

export interface KworbGlobalArtistRow {
  rank: number;
  artist: string;
  points: number;
}

export interface KworbGlobalArtistPayload {
  label: string; // e.g., "Global Digital Artist Ranking | 2025-10-21 15:35 EDT"
  fetchedAtISO: string; // when *we* fetched
  pageTimeISO?: string; // parsed from page if we can normalize it
  rows: KworbGlobalArtistRow[];
}

export interface KworbAppleAlbumRow {
  rank: number;
  artist: string;
  album: string; // <- formerly title
}

export interface KworbAppleAlbumsPayload {
  country: string; // e.g. 'ng'
  label: string; // e.g. 'Nigeria Apple Music Top Albums | 14:20 EDT'
  fetchedAtISO: string;
  pageTimeISO?: string;
  rows: KworbAppleAlbumRow[];
}

export type KworbSpotifyDailyRow = {
  rank: number;
  artist: string;
  title: string;
  featuredArtists: string[];
};

export type KworbSpotifyDailyPayload = {
  country: string;
  label: string;
  fetchedAtISO: string;
  pageTimeISO?: string;
  chartDateISO?: string;
  rows: KworbSpotifyDailyRow[];
};
