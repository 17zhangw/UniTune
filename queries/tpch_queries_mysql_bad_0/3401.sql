SELECT PARTSUPP.PS_SUPPLYCOST, PART.P_PARTKEY FROM PARTSUPP JOIN PART ON PART.P_PARTKEY=PARTSUPP.PS_PARTKEY;