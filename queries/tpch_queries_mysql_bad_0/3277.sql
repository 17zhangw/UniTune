SELECT ORDERS.O_TOTALPRICE, LINEITEM.L_DISCOUNT, CUSTOMER.C_MKTSEGMENT FROM CUSTOMER JOIN ORDERS ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY JOIN LINEITEM ON LINEITEM.L_ORDERKEY=ORDERS.O_ORDERKEY WHERE LINEITEM.L_COMMITDATE >= '1994-02-03' ORDER BY ORDERS.O_TOTALPRICE ASC, CUSTOMER.C_MKTSEGMENT ASC;