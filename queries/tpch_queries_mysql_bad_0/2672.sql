SELECT SUPPLIER.S_NAME, NATION.N_COMMENT FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY WHERE NATION.N_REGIONKEY >= 11256.36;