SELECT CUSTOMER.C_ACCTBAL, NATION.N_NATIONKEY, MAX(NATION.N_NAME) FROM CUSTOMER JOIN NATION ON NATION.N_NATIONKEY=CUSTOMER.C_NATIONKEY GROUP BY CUSTOMER.C_ACCTBAL, NATION.N_NATIONKEY ORDER BY MAX(NATION.N_NAME) DESC;