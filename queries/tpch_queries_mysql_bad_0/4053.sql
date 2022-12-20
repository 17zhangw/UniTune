SELECT SUPPLIER.S_COMMENT, NATION.N_NAME, MAX(SUPPLIER.S_ADDRESS) FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY GROUP BY SUPPLIER.S_COMMENT, NATION.N_NAME HAVING MAX(SUPPLIER.S_ADDRESS) <= 'TQXUVM7S7CNK';