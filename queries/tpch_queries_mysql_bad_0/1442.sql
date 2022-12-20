SELECT PART.P_CONTAINER, SUPPLIER.S_COMMENT, PARTSUPP.PS_SUPPLYCOST, MIN(SUPPLIER.S_ACCTBAL) FROM PART JOIN PARTSUPP ON PARTSUPP.PS_PARTKEY=PART.P_PARTKEY JOIN SUPPLIER ON SUPPLIER.S_SUPPKEY=PARTSUPP.PS_SUPPKEY GROUP BY PART.P_CONTAINER, SUPPLIER.S_COMMENT, PARTSUPP.PS_SUPPLYCOST;