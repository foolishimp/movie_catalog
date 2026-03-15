-- Media Catalog Schema

CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- fuzzy text search

CREATE TYPE media_type AS ENUM ('movie', 'series', 'unknown');

CREATE TABLE media (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    year            INTEGER,
    media_type      media_type DEFAULT 'unknown',
    -- parsed from filename
    parsed_title    TEXT,
    season          INTEGER,
    episode         INTEGER,
    resolution      TEXT,
    codec           TEXT,
    source          TEXT,          -- bluray, web-dl, hdtv etc.
    release_group   TEXT,
    -- file info
    file_path       TEXT NOT NULL UNIQUE,
    file_name       TEXT NOT NULL,
    file_size_bytes BIGINT,
    file_ext        TEXT,
    directory       TEXT,
    -- tmdb metadata
    tmdb_id         INTEGER,
    imdb_id         TEXT,
    overview        TEXT,
    genres          TEXT[],
    vote_average    REAL,
    vote_count      INTEGER,
    poster_path     TEXT,
    backdrop_path   TEXT,
    release_date    TEXT,
    original_language TEXT,
    popularity      REAL,
    cast_names      TEXT[],
    director        TEXT,
    tagline         TEXT,
    runtime_minutes INTEGER,
    status          TEXT,           -- released, ended, returning series etc.
    -- catalog metadata
    tags            TEXT[] DEFAULT '{}',
    notes           TEXT,
    duplicate_group TEXT,           -- hash grouping duplicates together
    enriched_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for fast search
CREATE INDEX idx_media_title_trgm ON media USING gin (title gin_trgm_ops);
CREATE INDEX idx_media_parsed_title_trgm ON media USING gin (parsed_title gin_trgm_ops);
CREATE INDEX idx_media_genres ON media USING gin (genres);
CREATE INDEX idx_media_tags ON media USING gin (tags);
CREATE INDEX idx_media_year ON media (year);
CREATE INDEX idx_media_media_type ON media (media_type);
CREATE INDEX idx_media_tmdb_id ON media (tmdb_id);
CREATE INDEX idx_media_duplicate_group ON media (duplicate_group);
CREATE INDEX idx_media_directory ON media (directory);
CREATE INDEX idx_media_file_size ON media (file_size_bytes);

-- Full text search
ALTER TABLE media ADD COLUMN tsv tsvector;
CREATE INDEX idx_media_tsv ON media USING gin (tsv);

CREATE OR REPLACE FUNCTION media_tsv_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.tsv :=
    setweight(to_tsvector('english', coalesce(NEW.title, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(NEW.overview, '')), 'B') ||
    setweight(to_tsvector('english', coalesce(array_to_string(NEW.genres, ' '), '')), 'C') ||
    setweight(to_tsvector('english', coalesce(NEW.director, '')), 'C');
  RETURN NEW;
END;
$$;

CREATE TRIGGER media_tsv_trig BEFORE INSERT OR UPDATE ON media
  FOR EACH ROW EXECUTE FUNCTION media_tsv_update();

-- Duplicate detection view: same title+year appearing in multiple paths
CREATE VIEW duplicate_candidates AS
SELECT
    lower(trim(parsed_title)) AS norm_title,
    year,
    media_type,
    count(*) AS copy_count,
    array_agg(file_path ORDER BY file_size_bytes DESC) AS paths,
    array_agg(file_size_bytes ORDER BY file_size_bytes DESC) AS sizes,
    array_agg(resolution ORDER BY file_size_bytes DESC) AS resolutions,
    array_agg(id ORDER BY file_size_bytes DESC) AS ids
FROM media
WHERE parsed_title IS NOT NULL
GROUP BY lower(trim(parsed_title)), year, media_type
HAVING count(*) > 1;

-- Handy stats view
CREATE VIEW catalog_stats AS
SELECT
    count(*) AS total_entries,
    count(*) FILTER (WHERE media_type = 'movie') AS movies,
    count(*) FILTER (WHERE media_type = 'series') AS series,
    count(*) FILTER (WHERE tmdb_id IS NOT NULL) AS enriched,
    count(*) FILTER (WHERE tmdb_id IS NULL) AS unenriched,
    (SELECT count(*) FROM duplicate_candidates) AS duplicate_groups,
    pg_size_pretty(sum(file_size_bytes)) AS total_size
FROM media;
