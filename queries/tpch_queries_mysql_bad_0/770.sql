SELECT ORDERS.O_CUSTKEY, CUSTOMER.C_NAME FROM ORDERS JOIN CUSTOMER ON CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY;