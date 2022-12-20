SELECT CUSTOMER.C_ACCTBAL, NATION.N_NATIONKEY, SUPPLIER.S_SUPPKEY, MIN(SUPPLIER.S_ADDRESS) FROM CUSTOMER, NATION, SUPPLIER WHERE CUSTOMER.C_ACCTBAL <= 7838.30 AND CUSTOMER.C_NATIONKEY < 16 GROUP BY CUSTOMER.C_ACCTBAL, NATION.N_NATIONKEY, SUPPLIER.S_SUPPKEY HAVING MIN(SUPPLIER.S_ADDRESS) != '1KHUGZEGWM3UA7DSYMEKYBSK';