-- IMDb quickstart source data, preloaded on first boot.
--
-- Two schemas make the CQRS split visible: `ingress` holds the normalized
-- source tables committed ingests FROM (you never query these); `read` is where
-- the denormalized movie_card lands and where you run your queries.
CREATE SCHEMA ingress;
CREATE SCHEMA read;

CREATE TABLE ingress.title (
  tconst        text PRIMARY KEY,
  primary_title text,
  start_year    int,
  genres        text
);
CREATE TABLE ingress.rating (
  tconst         text PRIMARY KEY,
  average_rating numeric,
  num_votes      int
);
CREATE TABLE ingress.principal (
  tconst   text,
  ordering int,
  nconst   text,
  category text,
  PRIMARY KEY (tconst, ordering)
);
CREATE TABLE ingress.name (
  nconst       text PRIMARY KEY,
  primary_name text
);

INSERT INTO ingress.title VALUES
 ('tt0111161','The Shawshank Redemption',1994,'Drama'),
 ('tt0068646','The Godfather',1972,'Crime,Drama'),
 ('tt0468569','The Dark Knight',2008,'Action,Crime,Drama');

INSERT INTO ingress.rating VALUES
 ('tt0111161',9.3,2800000),
 ('tt0068646',9.2,1900000),
 ('tt0468569',9.0,2700000);

-- Per movie: two billed actors (ordering 1,2) and a director (ordering 3).
INSERT INTO ingress.principal VALUES
 ('tt0111161',1,'nm0000209','actor'),
 ('tt0111161',2,'nm0000151','actor'),
 ('tt0111161',3,'nm0001104','director'),
 ('tt0068646',1,'nm0000008','actor'),
 ('tt0068646',2,'nm0000199','actor'),
 ('tt0068646',3,'nm0000338','director'),
 ('tt0468569',1,'nm0000288','actor'),
 ('tt0468569',2,'nm0005132','actor'),
 ('tt0468569',3,'nm0634240','director');

INSERT INTO ingress.name VALUES
 ('nm0000209','Tim Robbins'),
 ('nm0000151','Morgan Freeman'),
 ('nm0001104','Frank Darabont'),
 ('nm0000008','Marlon Brando'),
 ('nm0000199','Al Pacino'),
 ('nm0000338','Francis Ford Coppola'),
 ('nm0000288','Christian Bale'),
 ('nm0005132','Heath Ledger'),
 ('nm0634240','Christopher Nolan');

-- Logical replication needs the key on UPDATE/DELETE; FULL is simplest for a
-- demo and lets the optional "live change" step work.
ALTER TABLE ingress.title     REPLICA IDENTITY FULL;
ALTER TABLE ingress.rating    REPLICA IDENTITY FULL;
ALTER TABLE ingress.principal REPLICA IDENTITY FULL;
ALTER TABLE ingress.name      REPLICA IDENTITY FULL;
