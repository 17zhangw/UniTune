SELECT PARTSUPP.PS_AVAILQTY, PART.P_MFGR FROM PART JOIN PARTSUPP ON PARTSUPP.PS_PARTKEY=PART.P_PARTKEY ORDER BY PARTSUPP.PS_AVAILQTY DESC;