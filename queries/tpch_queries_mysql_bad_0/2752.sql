SELECT LINEITEM.L_PARTKEY, ORDERS.O_ORDERKEY FROM LINEITEM JOIN ORDERS ON ORDERS.O_ORDERKEY=LINEITEM.L_ORDERKEY;