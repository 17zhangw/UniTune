SELECT ORDERS.O_CUSTKEY, NATION.N_REGIONKEY, CUSTOMER.C_CUSTKEY FROM NATION JOIN CUSTOMER ON CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY JOIN ORDERS ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY WHERE CUSTOMER.C_ACCTBAL != 7462.99 AND ORDERS.O_CLERK >= 'CLERK#000000700';