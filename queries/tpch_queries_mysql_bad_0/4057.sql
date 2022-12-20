SELECT CUSTOMER.C_ADDRESS, ORDERS.O_COMMENT, SUM(CUSTOMER.C_CUSTKEY) FROM ORDERS, CUSTOMER GROUP BY CUSTOMER.C_ADDRESS, ORDERS.O_COMMENT HAVING SUM(CUSTOMER.C_CUSTKEY) >= 26;