SELECT ORDERS.O_CUSTKEY, CUSTOMER.C_MKTSEGMENT FROM CUSTOMER, ORDERS ORDER BY CUSTOMER.C_MKTSEGMENT DESC, ORDERS.O_CUSTKEY DESC;