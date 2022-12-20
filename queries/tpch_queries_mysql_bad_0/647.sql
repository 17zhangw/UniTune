SELECT PART.P_CONTAINER, SUPPLIER.S_ACCTBAL, PARTSUPP.PS_SUPPKEY, MIN(SUPPLIER.S_SUPPKEY), MIN(SUPPLIER.S_ADDRESS), COUNT(PARTSUPP.PS_PARTKEY) FROM SUPPLIER, PARTSUPP, PART GROUP BY PART.P_CONTAINER, SUPPLIER.S_ACCTBAL, PARTSUPP.PS_SUPPKEY ORDER BY MIN(SUPPLIER.S_SUPPKEY) ASC, MIN(SUPPLIER.S_ADDRESS) ASC;