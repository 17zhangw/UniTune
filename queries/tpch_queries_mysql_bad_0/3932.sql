SELECT CUSTOMER.C_PHONE, ORDERS.O_ORDERKEY, AVG(ORDERS.O_SHIPPRIORITY) FROM ORDERS, CUSTOMER WHERE ORDERS.O_CUSTKEY >= 13951 AND ORDERS.O_ORDERKEY <= 514 GROUP BY CUSTOMER.C_PHONE, ORDERS.O_ORDERKEY;