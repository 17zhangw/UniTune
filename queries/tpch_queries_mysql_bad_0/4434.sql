SELECT ORDERS.O_COMMENT, CUSTOMER.C_CUSTKEY, MIN(CUSTOMER.C_NATIONKEY), COUNT(CUSTOMER.C_PHONE) FROM CUSTOMER, ORDERS GROUP BY ORDERS.O_COMMENT, CUSTOMER.C_CUSTKEY;