-- This file contains basic tests to run against the write-ahead logging
-- implementation.
--
-- IMPORTANT NOTE:  THIS FILE CANNOT BE RUN STRAIGHT THROUGH WITH NANODB!!!
-- The file uses the CRASH command to stop NanoDB, so therefore it can't be
-- run straight through.  Rather, cut and paste the various operations, and
-- manually check the database state, until such a point when the NanoDB
-- testing harness is more sophisticated.
--
-- IMPORTANT NOTE:  Most of these tests require the "datafiles" directory to
-- be blown away before each test.  This forces the write-ahead log to be
-- created from scratch.

-----------------------------------------------------------------------------
-- Test 1:  Basic commit guarantee.  Insert rows, commit, crash DB, make sure
--          they're still there.

-- TODO:  DELETE datafiles DIRECTORY

CREATE TABLE testwal (
  a INTEGER,
  b VARCHAR(30),
  c FLOAT
);

BEGIN;
INSERT INTO testwal VALUES (1, 'abc', 1.2);
INSERT INTO testwal VALUES (2, 'defghi', -3.6);

SELECT * FROM testwal;  -- Should list both records

COMMIT;

SELECT * FROM testwal;  -- Should list both records

CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should list both records

BEGIN;
INSERT INTO testwal VALUES (3, 'jklmnopqrst', 5.5);

SELECT * FROM testwal;  -- Should list all three records

COMMIT;

SELECT * FROM testwal;  -- Should list all three records

CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should list all three records


-----------------------------------------------------------------------------
-- Test 2A:  Basic rollback.  Rolled-back changes shouldn't appear in database
--           after rollback, even after restart.

-- TODO:  DELETE datafiles DIRECTORY

CREATE TABLE testwal (
  a INTEGER,
  b VARCHAR(30),
  c FLOAT
);

BEGIN;
INSERT INTO testwal VALUES (1, 'abc', 1.2);
INSERT INTO testwal VALUES (2, 'defghi', -3.6);
SELECT * FROM testwal;  -- Should list both records
COMMIT;
SELECT * FROM testwal;  -- Should list both records

BEGIN;
INSERT INTO testwal VALUES (-1, 'zxywvu', 78.2);

SELECT * FROM testwal;  -- Should list all three records

ROLLBACK;

SELECT * FROM testwal;  -- Should only list original two records

CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should only list original two records

INSERT INTO testwal VALUES (4, 'hmm hmm', 261.32);  -- Autocommits.

SELECT * FROM testwal;  -- Should list appropriate three records

CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should list appropriate three records

-----------------------------------------------------------------------------
-- Test 2B:  Basic rollback and recovery.  Identical to Test 2A, but flush
--           all data before crashing.

-- TODO:  DELETE datafiles DIRECTORY

CREATE TABLE testwal (
  a INTEGER,
  b VARCHAR(30),
  c FLOAT
);

BEGIN;
INSERT INTO testwal VALUES (1, 'abc', 1.2);
INSERT INTO testwal VALUES (2, 'defghi', -3.6);
SELECT * FROM testwal;  -- Should list both records
COMMIT;
SELECT * FROM testwal;  -- Should list both records

BEGIN;
INSERT INTO testwal VALUES (-1, 'zxywvu', 78.2);

SELECT * FROM testwal;  -- Should list all three records

ROLLBACK;

SELECT * FROM testwal;  -- Should only list original two records

FLUSH;
CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should only list original two records

INSERT INTO testwal VALUES (4, 'hmm hmm', 261.32);  -- Autocommits.

SELECT * FROM testwal;  -- Should list appropriate three records

FLUSH;
CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should list appropriate three records

-----------------------------------------------------------------------------
-- Test 2C:  Automatic rollback during recovery.  Identical to Test 2A, but
--           flush and crash instead of rolling back.

-- TODO:  DELETE datafiles DIRECTORY

CREATE TABLE testwal (
  a INTEGER,
  b VARCHAR(30),
  c FLOAT
);

BEGIN;
INSERT INTO testwal VALUES (1, 'abc', 1.2);
INSERT INTO testwal VALUES (2, 'defghi', -3.6);
SELECT * FROM testwal;  -- Should list both records
COMMIT;
SELECT * FROM testwal;  -- Should list both records

BEGIN;
INSERT INTO testwal VALUES (-1, 'zxywvu', 78.2);

SELECT * FROM testwal;  -- Should list all three records

ROLLBACK;

SELECT * FROM testwal;  -- Should only list original two records

FLUSH;
CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should only list original two records

INSERT INTO testwal VALUES (4, 'hmm hmm', 261.32);  -- Autocommits.

SELECT * FROM testwal;  -- Should list appropriate three records

FLUSH;
CRASH;

-- TODO:  RESTART NANODB

SELECT * FROM testwal;  -- Should list appropriate three records

