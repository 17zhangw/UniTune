SELECT NATION.N_NAME, REGION.R_COMMENT, CUSTOMER.C_ACCTBAL FROM CUSTOMER, NATION, REGION ORDER BY NATION.N_NAME DESC, CUSTOMER.C_ACCTBAL ASC;