SELECT CUSTOMER.C_PHONE, ORDERS.O_ORDERPRIORITY FROM ORDERS JOIN CUSTOMER ON CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY;