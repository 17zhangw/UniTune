SELECT ORDERS.O_CLERK, MAX(ORDERS.O_ORDERSTATUS) FROM ORDERS GROUP BY ORDERS.O_CLERK;