SELECT NATION.N_REGIONKEY, CUSTOMER.C_ADDRESS, SUPPLIER.S_ADDRESS, MIN(SUPPLIER.S_SUPPKEY) FROM SUPPLIER, NATION, CUSTOMER GROUP BY NATION.N_REGIONKEY, CUSTOMER.C_ADDRESS, SUPPLIER.S_ADDRESS ORDER BY NATION.N_REGIONKEY DESC, SUPPLIER.S_ADDRESS DESC, CUSTOMER.C_ADDRESS DESC;