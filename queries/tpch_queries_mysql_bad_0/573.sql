SELECT LINEITEM.L_TAX, PARTSUPP.PS_COMMENT, ORDERS.O_ORDERPRIORITY FROM ORDERS JOIN LINEITEM ON LINEITEM.L_ORDERKEY=ORDERS.O_ORDERKEY JOIN PARTSUPP ON PARTSUPP.PS_SUPPKEY=LINEITEM.L_SUPPKEY;