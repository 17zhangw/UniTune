SELECT SUPPLIER.S_SUPPKEY, NATION.N_REGIONKEY FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY ORDER BY NATION.N_REGIONKEY ASC, SUPPLIER.S_SUPPKEY ASC;