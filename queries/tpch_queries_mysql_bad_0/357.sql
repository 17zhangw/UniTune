SELECT CUSTOMER.C_ACCTBAL, MAX(CUSTOMER.C_COMMENT), COUNT(CUSTOMER.C_MKTSEGMENT) FROM CUSTOMER GROUP BY CUSTOMER.C_ACCTBAL;