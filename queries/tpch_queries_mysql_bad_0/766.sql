SELECT SUPPLIER.S_NAME, NATION.N_COMMENT, MAX(NATION.N_NAME) FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY GROUP BY SUPPLIER.S_NAME, NATION.N_COMMENT;