SELECT SUPPLIER.S_COMMENT, NATION.N_COMMENT FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY WHERE NATION.N_NAME > '16145.16';