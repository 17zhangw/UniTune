SELECT ORDERS.O_ORDERSTATUS, CUSTOMER.C_ADDRESS, PARTSUPP.PS_SUPPKEY, LINEITEM.L_PARTKEY FROM CUSTOMER, ORDERS, LINEITEM, PARTSUPP ORDER BY PARTSUPP.PS_SUPPKEY ASC, ORDERS.O_ORDERSTATUS DESC;