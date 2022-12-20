SELECT CUSTOMER.C_NATIONKEY, ORDERS.O_ORDERKEY, SUM(ORDERS.O_CUSTKEY), COUNT(ORDERS.O_ORDERKEY), COUNT(ORDERS.O_COMMENT) FROM CUSTOMER, ORDERS GROUP BY CUSTOMER.C_NATIONKEY, ORDERS.O_ORDERKEY HAVING COUNT(ORDERS.O_COMMENT) >= 2 AND COUNT(ORDERS.O_ORDERKEY) >= 7 ORDER BY CUSTOMER.C_NATIONKEY DESC, ORDERS.O_ORDERKEY DESC;