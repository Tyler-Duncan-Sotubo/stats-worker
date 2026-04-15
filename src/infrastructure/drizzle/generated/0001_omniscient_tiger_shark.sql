CREATE TABLE "artist_monthly_listener_snapshots" (
	"id" uuid PRIMARY KEY NOT NULL,
	"artist_id" uuid NOT NULL,
	"spotify_id" text NOT NULL,
	"snapshot_date" date NOT NULL,
	"monthly_listeners" bigint NOT NULL,
	"daily_change" bigint,
	"peak_rank" integer,
	"peak_listeners" bigint,
	"source" text DEFAULT 'kworb' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "artist_monthly_listener_snapshots" ADD CONSTRAINT "artist_monthly_listener_snapshots_artist_id_artists_id_fk" FOREIGN KEY ("artist_id") REFERENCES "public"."artists"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "uq_artist_monthly_listener_snapshot" ON "artist_monthly_listener_snapshots" USING btree ("artist_id","snapshot_date");--> statement-breakpoint
CREATE INDEX "idx_amls_snapshot_date" ON "artist_monthly_listener_snapshots" USING btree ("snapshot_date");--> statement-breakpoint
CREATE INDEX "idx_amls_monthly_listeners" ON "artist_monthly_listener_snapshots" USING btree ("snapshot_date","monthly_listeners");--> statement-breakpoint
CREATE INDEX "idx_amls_daily_change" ON "artist_monthly_listener_snapshots" USING btree ("snapshot_date","daily_change");--> statement-breakpoint
CREATE INDEX "idx_amls_artist" ON "artist_monthly_listener_snapshots" USING btree ("artist_id","snapshot_date");--> statement-breakpoint
CREATE INDEX "idx_amls_spotify_id" ON "artist_monthly_listener_snapshots" USING btree ("spotify_id");