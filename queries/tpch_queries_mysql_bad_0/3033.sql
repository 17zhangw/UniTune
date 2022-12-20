SELECT NATION.N_NATIONKEY, CUSTOMER.C_NATIONKEY, COUNT(CUSTOMER.C_PHONE) FROM NATION, CUSTOMER GROUP BY NATION.N_NATIONKEY, CUSTOMER.C_NATIONKEY HAVING COUNT(CUSTOMER.C_PHONE) <= 1 ORDER BY CUSTOMER.C_NATIONKEY DESC, NATION.N_NATIONKEY DESC;