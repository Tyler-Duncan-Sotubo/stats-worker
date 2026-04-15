import { Module } from '@nestjs/common';
import { AlbumsRepository } from './albums.repository';
import { ArtistsRepository } from './artists.repository';
import { SongsRepository } from './songs.repository';
import { CertificationsRepository } from './certifications.repository';
import { SnapshotRepository } from './snapshot.repository';

@Module({
  providers: [
    AlbumsRepository,
    ArtistsRepository,
    SongsRepository,
    CertificationsRepository,
    SnapshotRepository,
    SongsRepository,
  ],
  exports: [
    AlbumsRepository,
    ArtistsRepository,
    SongsRepository,
    CertificationsRepository,
    SnapshotRepository,
    SongsRepository,
  ],
})
export class RepositoryModule {}
