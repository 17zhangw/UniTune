SELECT LINEITEM.L_RETURNFLAG, PARTSUPP.PS_SUPPLYCOST, MAX(LINEITEM.L_COMMITDATE) FROM LINEITEM, PARTSUPP WHERE LINEITEM.L_RETURNFLAG <= 'R' GROUP BY LINEITEM.L_RETURNFLAG, PARTSUPP.PS_SUPPLYCOST ORDER BY LINEITEM.L_RETURNFLAG ASC, PARTSUPP.PS_SUPPLYCOST DESC;