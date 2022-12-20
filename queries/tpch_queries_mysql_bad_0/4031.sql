SELECT LINEITEM.L_QUANTITY, ORDERS.O_CUSTKEY, CUSTOMER.C_NAME, MIN(LINEITEM.L_SHIPINSTRUCT) FROM LINEITEM, ORDERS, CUSTOMER WHERE LINEITEM.L_QUANTITY < 27 GROUP BY LINEITEM.L_QUANTITY, ORDERS.O_CUSTKEY, CUSTOMER.C_NAME ORDER BY CUSTOMER.C_NAME DESC;