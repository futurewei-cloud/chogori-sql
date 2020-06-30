--
-- KEY Pushdown Processing.
-- This file tests key-pushdown for system table scan.
--
-- Different from UserTable, system tables and its indexes are centralized in one tablet. To take
-- advantage of this fact, systable-scan queries the data using an INDEX key in one operation.
-- Normally it'd take two operations, one to select ROWID and another to select actual data.
--
-- Test forward scan.
EXPLAIN (COSTS OFF) SELECT classid, objid, objsubid, refclassid, refobjid, deptype FROM pg_depend
		WHERE deptype != 'p' AND deptype != 'e' AND deptype != 'i'
		ORDER BY classid, objid, objsubid
		LIMIT 2;

-- We cannot run the following SELECT on test because different platforms have different system
-- catalog data.
--
-- SELECT classid, objid, objsubid, refclassid, refobjid, deptype FROM pg_depend
--		WHERE deptype != 'p' AND deptype != 'e' AND deptype != 'i'
--		ORDER BY classid, objid, objsubid
--		LIMIT 2;

-- Test reverse scan.
EXPLAIN (COSTS OFF) SELECT classid, objid, objsubid, refclassid, refobjid, deptype FROM pg_depend
		WHERE deptype != 'p' AND deptype != 'e' AND deptype != 'i'
		ORDER BY classid DESC, objid DESC, objsubid DESC
		LIMIT 2;

-- We cannot run the following SELECT on test because different platforms have different system
-- catalog data.
--
-- SELECT classid, objid, objsubid, refclassid, refobjid, deptype FROM pg_depend
--		WHERE deptype != 'p' AND deptype != 'e' AND deptype != 'i'
--		ORDER BY classid DESC, objid DESC, objsubid DESC
--		LIMIT 2;
