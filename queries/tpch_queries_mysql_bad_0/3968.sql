SELECT PARTSUPP.PS_SUPPLYCOST, SUPPLIER.S_NATIONKEY, LINEITEM.L_LINENUMBER, NATION.N_COMMENT FROM NATION JOIN SUPPLIER ON SUPPLIER.S_NATIONKEY=NATION.N_NATIONKEY JOIN PARTSUPP ON PARTSUPP.PS_SUPPKEY=SUPPLIER.S_SUPPKEY JOIN LINEITEM ON LINEITEM.L_SUPPKEY=PARTSUPP.PS_SUPPKEY ORDER BY SUPPLIER.S_NATIONKEY ASC, PARTSUPP.PS_SUPPLYCOST ASC;