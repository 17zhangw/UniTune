SELECT NATION.N_COMMENT, SUPPLIER.S_NATIONKEY, MIN(NATION.N_NAME) FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY GROUP BY NATION.N_COMMENT, SUPPLIER.S_NATIONKEY;