SELECT PART.P_NAME, PARTSUPP.PS_PARTKEY, SUPPLIER.S_SUPPKEY FROM SUPPLIER, PARTSUPP, PART ORDER BY PARTSUPP.PS_PARTKEY DESC, SUPPLIER.S_SUPPKEY ASC, PART.P_NAME DESC;