SELECT ORDERS.O_CUSTKEY, CUSTOMER.C_NATIONKEY FROM CUSTOMER, ORDERS WHERE ORDERS.O_ORDERPRIORITY >= '3-MEDIUM       ' ORDER BY CUSTOMER.C_NATIONKEY DESC;