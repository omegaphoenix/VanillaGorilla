-- Complete transaction.
BEGIN;
INSERT INTO t values (1);
INSERT INTO t values (2);
COMMIT;

-- Transaction that will have to be undone.
BEGIN;
INSERT INTO t values (-1);
INSERT INTO t values (-2);
SELECT * FROM t;
CRASH 10;
