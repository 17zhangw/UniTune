SELECT CUSTOMER.C_CUSTKEY, NATION.N_COMMENT FROM NATION, CUSTOMER WHERE CUSTOMER.C_MKTSEGMENT <= 'HOUSEHOLD ' ORDER BY NATION.N_COMMENT ASC, CUSTOMER.C_CUSTKEY DESC;