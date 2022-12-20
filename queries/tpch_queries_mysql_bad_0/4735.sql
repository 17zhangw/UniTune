SELECT CUSTOMER.C_MKTSEGMENT, ORDERS.O_ORDERPRIORITY FROM CUSTOMER JOIN ORDERS ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY WHERE CUSTOMER.C_CUSTKEY > 87 AND ORDERS.O_SHIPPRIORITY >= 0 ORDER BY ORDERS.O_ORDERPRIORITY ASC, CUSTOMER.C_MKTSEGMENT ASC;