SELECT NATION.N_NATIONKEY, CUSTOMER.C_COMMENT FROM NATION JOIN CUSTOMER ON CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY WHERE CUSTOMER.C_ACCTBAL > 4113.64 ORDER BY NATION.N_NATIONKEY ASC;