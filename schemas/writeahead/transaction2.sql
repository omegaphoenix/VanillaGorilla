-- Complete transaction.
BEGIN;
INSERT INTO t values (30);
INSERT INTO t values (40);
COMMIT;

-- Transaction that will have to be undone.
BEGIN;
INSERT INTO t values (-30);
INSERT INTO t values (-40);
SELECT * FROM t;
CRASH;
