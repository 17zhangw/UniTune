SELECT LINEITEM.L_TAX, PARTSUPP.PS_SUPPLYCOST, ORDERS.O_CUSTKEY, SUM(PARTSUPP.PS_SUPPKEY) FROM PARTSUPP, LINEITEM, ORDERS GROUP BY LINEITEM.L_TAX, PARTSUPP.PS_SUPPLYCOST, ORDERS.O_CUSTKEY ORDER BY SUM(PARTSUPP.PS_SUPPKEY) ASC;