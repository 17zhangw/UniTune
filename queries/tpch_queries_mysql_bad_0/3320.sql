SELECT PARTSUPP.PS_SUPPKEY, ORDERS.O_CUSTKEY, PART.P_PARTKEY, LINEITEM.L_DISCOUNT FROM ORDERS, LINEITEM, PARTSUPP, PART ORDER BY LINEITEM.L_DISCOUNT DESC, PARTSUPP.PS_SUPPKEY DESC;