SELECT LINEITEM.L_DISCOUNT, ORDERS.O_TOTALPRICE, PARTSUPP.PS_SUPPKEY FROM PARTSUPP, LINEITEM, ORDERS WHERE ORDERS.O_ORDERPRIORITY = '2-HIGH         ';