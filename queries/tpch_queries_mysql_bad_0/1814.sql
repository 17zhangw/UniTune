SELECT NATION.N_NAME, SUPPLIER.S_ACCTBAL, COUNT(NATION.N_COMMENT), MIN(SUPPLIER.S_NAME) FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY GROUP BY NATION.N_NAME, SUPPLIER.S_ACCTBAL HAVING MIN(SUPPLIER.S_NAME) >= 'SUPPLIER#000000004       ';