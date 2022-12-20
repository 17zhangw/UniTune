SELECT CUSTOMER.C_COMMENT, PART.P_PARTKEY, ORDERS.O_ORDERDATE, PARTSUPP.PS_SUPPLYCOST, LINEITEM.L_PARTKEY FROM CUSTOMER JOIN ORDERS ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY JOIN LINEITEM ON LINEITEM.L_ORDERKEY=ORDERS.O_ORDERKEY JOIN PARTSUPP ON PARTSUPP.PS_SUPPKEY=LINEITEM.L_SUPPKEY JOIN PART ON PART.P_PARTKEY=PARTSUPP.PS_PARTKEY ORDER BY PART.P_PARTKEY DESC;