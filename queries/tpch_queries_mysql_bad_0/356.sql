SELECT LINEITEM.L_RETURNFLAG, PARTSUPP.PS_SUPPKEY, SUM(LINEITEM.L_EXTENDEDPRICE), COUNT(PARTSUPP.PS_COMMENT) FROM PARTSUPP, LINEITEM GROUP BY LINEITEM.L_RETURNFLAG, PARTSUPP.PS_SUPPKEY ORDER BY COUNT(PARTSUPP.PS_COMMENT) DESC, SUM(LINEITEM.L_EXTENDEDPRICE) DESC;