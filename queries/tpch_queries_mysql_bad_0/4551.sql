SELECT SUPPLIER.S_NATIONKEY, PARTSUPP.PS_SUPPLYCOST FROM PARTSUPP JOIN SUPPLIER ON SUPPLIER.S_SUPPKEY=PARTSUPP.PS_SUPPKEY ORDER BY SUPPLIER.S_NATIONKEY ASC, PARTSUPP.PS_SUPPLYCOST ASC;