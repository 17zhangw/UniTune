SELECT ORDERS.O_COMMENT, LINEITEM.L_SHIPDATE FROM LINEITEM, ORDERS WHERE ORDERS.O_TOTALPRICE <= 16375.79 AND ORDERS.O_ORDERSTATUS >= 'O';