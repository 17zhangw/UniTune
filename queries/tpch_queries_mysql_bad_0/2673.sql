SELECT ORDERS.O_CLERK, LINEITEM.L_SHIPDATE FROM LINEITEM, ORDERS WHERE ORDERS.O_ORDERSTATUS != 'P';