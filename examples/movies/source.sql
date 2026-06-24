-- Quickstart source data — a tiny, entirely fictional movie catalog, preloaded
-- on first boot. (Any resemblance to real films is a pun, not a fact.)
--
-- Two schemas make the CQRS split visible: `ingress` holds the normalized source
-- tables committed ingests FROM (you never query these); `read` is where the
-- denormalized movie_card lands and where you run your queries.
CREATE SCHEMA ingress;
CREATE SCHEMA read;

CREATE TABLE ingress.movie (
  movie_id text PRIMARY KEY,
  title    text,
  year     int,
  genres   text
);
CREATE TABLE ingress.rating (
  movie_id text PRIMARY KEY,
  score    numeric,
  votes    int
);
CREATE TABLE ingress.credit (
  movie_id  text,
  billing   int,
  person_id text,
  role      text,
  PRIMARY KEY (movie_id, billing)
);
CREATE TABLE ingress.person (
  person_id text PRIMARY KEY,
  name      text
);

INSERT INTO ingress.movie VALUES
 ('mv0000001','The Slawshank Redemption',1996,'Drama'),
 ('mv0000002','The Codfather',1974,'Crime,Drama'),
 ('mv0000003','The Bark Knight',2011,'Action,Crime,Drama');

INSERT INTO ingress.rating VALUES
 ('mv0000001',9.4,2600000),
 ('mv0000002',9.1,1800000),
 ('mv0000003',8.8,2500000);

-- Per movie: two billed actors (billing 1,2) and a director (billing 3).
INSERT INTO ingress.credit VALUES
 ('mv0000001',1,'pn0000001','actor'),
 ('mv0000001',2,'pn0000002','actor'),
 ('mv0000001',3,'pn0000003','director'),
 ('mv0000002',1,'pn0000004','actor'),
 ('mv0000002',2,'pn0000005','actor'),
 ('mv0000002',3,'pn0000006','director'),
 ('mv0000003',1,'pn0000007','actor'),
 ('mv0000003',2,'pn0000008','actor'),
 ('mv0000003',3,'pn0000009','director');

INSERT INTO ingress.person VALUES
 ('pn0000001','Tim Dobbins'),
 ('pn0000002','Organ Freeman'),
 ('pn0000003','Frank Marinara'),
 ('pn0000004','Marlin Blando'),
 ('pn0000005','Al Cappuccino'),
 ('pn0000006','Francis Fjord Cupola'),
 ('pn0000007','Christian Kale'),
 ('pn0000008','Heath Lager'),
 ('pn0000009','Christopher No-Land');

-- Logical replication needs the key on UPDATE/DELETE; FULL is simplest for a
-- demo and lets the optional "live change" step work.
ALTER TABLE ingress.movie  REPLICA IDENTITY FULL;
ALTER TABLE ingress.rating REPLICA IDENTITY FULL;
ALTER TABLE ingress.credit REPLICA IDENTITY FULL;
ALTER TABLE ingress.person REPLICA IDENTITY FULL;
