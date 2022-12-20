SELECT LINEITEM.L_LINESTATUS, PARTSUPP.PS_PARTKEY, ORDERS.O_SHIPPRIORITY FROM ORDERS, LINEITEM, PARTSUPP WHERE LINEITEM.L_SHIPMODE >= 'MAIL      ' ORDER BY ORDERS.O_SHIPPRIORITY ASC, LINEITEM.L_LINESTATUS DESC, PARTSUPP.PS_PARTKEY ASC;