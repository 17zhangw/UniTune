SELECT ORDERS.O_ORDERSTATUS, CUSTOMER.C_PHONE, NATION.N_REGIONKEY FROM NATION, CUSTOMER, ORDERS ORDER BY NATION.N_REGIONKEY DESC;