CREATE DATABASE lanldb ;

\c lanldb ;

CREATE TABLE auth  (
  time       INTEGER,
  suser      VARCHAR(50),
  duser      VARCHAR(50),
  shost      VARCHAR(50),
  dhost      VARCHAR(50),
  authtype   VARCHAR(50),
  logontype  VARCHAR(50),
  authorient VARCHAR(50),
  status     VARCHAR(50));

CREATE TABLE flows (
  time       INTEGER,
  duration   INTEGER,
  shost      VARCHAR(50),
  sport      VARCHAR(50),
  dhost      VARCHAR(50),
  dport      VARCHAR(50),
  protocol   VARCHAR(50),
  pktcount   BIGINT,
  bytecount  BIGINT
);

CREATE TABLE proc (
  time       INTEGER,
  "user"     VARCHAR(50),
  host       VARCHAR(50),
  procname   VARCHAR(50),
  startstop  VARCHAR(50)
  );

CREATE TABLE dns (
  time         INTEGER,
  shost        VARCHAR(50),
  resolvedhost VARCHAR(50)
);

CREATE TABLE redteam (
  time       INTEGER,
  "user"     VARCHAR(50),
  shost      VARCHAR(50),
  dhost      VARCHAR(50)
);

-- # Modify the path to the data files

COPY auth    FROM PROGRAM 'gzip -dc auth.txt.gz | head -100000000' DELIMITERS ',' CSV;
COPY proc    FROM PROGRAM 'gzip -dc proc.txt.gz | head -100000000' DELIMITERS ',' CSV;
COPY dns     FROM PROGRAM 'gzip -dc dns.txt.gz  | head -100000000' DELIMITERS ',' CSV;
COPY flows   FROM PROGRAM 'gzip -dc flows.txt.gz| head -100000000' DELIMITERS ',' CSV;
COPY redteam FROM PROGRAM 'gzip -dc redteam.txt.gz' DELIMITERS ',' CSV;
