SELECT ORDERS.O_ORDERSTATUS, CUSTOMER.C_ACCTBAL FROM CUSTOMER JOIN ORDERS ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY;