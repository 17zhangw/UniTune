SELECT CUSTOMER.C_PHONE, ORDERS.O_ORDERKEY, LINEITEM.L_SUPPKEY, NATION.N_NATIONKEY FROM LINEITEM, ORDERS, CUSTOMER, NATION ORDER BY LINEITEM.L_SUPPKEY ASC, NATION.N_NATIONKEY ASC, CUSTOMER.C_PHONE ASC;