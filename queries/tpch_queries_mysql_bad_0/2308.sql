SELECT SUPPLIER.S_ACCTBAL, MIN(SUPPLIER.S_ACCTBAL) FROM SUPPLIER GROUP BY SUPPLIER.S_ACCTBAL ORDER BY SUPPLIER.S_ACCTBAL DESC;