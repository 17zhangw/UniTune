SELECT SUPPLIER.S_NATIONKEY, PARTSUPP.PS_SUPPLYCOST FROM PARTSUPP JOIN SUPPLIER ON SUPPLIER.S_SUPPKEY=PARTSUPP.PS_SUPPKEY;