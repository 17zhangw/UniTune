SELECT CUSTOMER.C_MKTSEGMENT, ORDERS.O_ORDERSTATUS, COUNT(ORDERS.O_ORDERPRIORITY) FROM ORDERS, CUSTOMER GROUP BY CUSTOMER.C_MKTSEGMENT, ORDERS.O_ORDERSTATUS ORDER BY COUNT(ORDERS.O_ORDERPRIORITY) ASC;