SELECT LINEITEM.L_SHIPINSTRUCT, ORDERS.O_SHIPPRIORITY FROM LINEITEM, ORDERS WHERE ORDERS.O_ORDERKEY > 1093 ORDER BY LINEITEM.L_SHIPINSTRUCT ASC;