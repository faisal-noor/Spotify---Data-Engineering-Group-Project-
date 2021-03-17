CREATE SCHEMA IF NOT EXISTS spotify;

DROP TABLE IF EXISTS spotify.artists;
CREATE TABLE IF NOT EXISTS spotify.artists (
    artist_id     varchar(256) primary key,
    artist_name   varchar(256),
    followers     varchar(256),
    popularity    varchar(256)
);

DROP TABLE IF EXISTS spotify.albums;
CREATE TABLE IF NOT EXISTS spotify.albums (
    album_id       varchar(256) primary key,
    artist_id      varchar(256) references spotify.artists(artist_id),
    album_group    varchar(256),
    album_type     varchar(256),
    album_name     varchar(256)
);

DROP TABLE IF EXISTS spotify.albums_images_links;
CREATE TABLE IF NOT EXISTS spotify.albums_images_links (
    album_id     varchar(256) primary key references spotify.albums(album_id),
    image_number int,
    height       int,
    width        int,
    url          varchar(256)
);

DROP TABLE IF EXISTS spotify.tracks;
CREATE TABLE IF NOT EXISTS spotify.tracks (
    track_id     varchar(256) primary key ,
  album_id     varchar(256) references spotify.albums(album_id),
  disc_number  int,
  duration_ms  numeric,
  explicit     boolean,
  is_local     boolean,
  track_name   varchar(256),
  preview_url  varchar(256),
  track_number int,
  track_type   varchar(256)
);

DROP TABLE IF EXISTS spotify.audio_features;
CREATE TABLE IF NOT EXISTS spotify.audio_features (
    track_id        varchar(256) primary key references spotify.tracks(track_id),
    danceability    numeric,
    energy          numeric,
    key             int,
  loudness        numeric,
  mode            int,
  speechiness     numeric,
  acousticness    numeric,
  intrumentalness numeric,
  liveness        numeric,
  valence         numeric,
  tempo           numeric,
  register_type   varchar(256),
  duration_ms     numeric,
  time_signature  int
);

DROP TABLE IF EXISTS spotify.lyrics;
CREATE TABLE IF NOT EXISTS spotify.lyrics (
  artist_name varchar(256),
  track_name  varchar(256),
  track_id    varchar(256) references spotify.tracks(track_id),
  lyrics      varchar(10485760)
);