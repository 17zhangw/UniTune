SELECT PART.P_TYPE, PARTSUPP.PS_PARTKEY, MIN(PART.P_NAME), MIN(PART.P_MFGR) FROM PART, PARTSUPP GROUP BY PART.P_TYPE, PARTSUPP.PS_PARTKEY ORDER BY MIN(PART.P_MFGR) ASC;