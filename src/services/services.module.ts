import { Module } from '@nestjs/common';
import { AlbumScraperService } from './album-scraper.service';
import { ArtistsService } from './artists.service';
import { CertificationsService } from './certifications.service';
import { EntityResolutionService } from './entity-resolution.service';
import { SnapshotService } from './snapshot.service';
import { SongScraperService } from './song-scraper.service';
import { ViewsRefreshService } from './views-refresh.service';
import { RepositoryModule } from 'src/repository/repository.module';
import { SpotifyMetadataService } from 'src/scraper/services/spotify-metadata.service';
import { RiaaCertificationService } from 'src/scraper/services/riaa-certification.service';
import { ScraperModule } from 'src/scraper/scraper.module';

@Module({
  imports: [RepositoryModule, ScraperModule],
  providers: [
    AlbumScraperService,
    ArtistsService,
    CertificationsService,
    EntityResolutionService,
    SnapshotService,
    SongScraperService,
    ViewsRefreshService,
    SpotifyMetadataService,
    RiaaCertificationService,
  ],
  exports: [
    AlbumScraperService,
    ArtistsService,
    CertificationsService,
    EntityResolutionService,
    SnapshotService,
    SongScraperService,
    ViewsRefreshService,
  ],
})
export class ServicesModule {}
