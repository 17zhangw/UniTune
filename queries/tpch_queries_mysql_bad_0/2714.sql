SELECT LINEITEM.L_SUPPKEY, PARTSUPP.PS_SUPPLYCOST, PART.P_PARTKEY, MAX(PART.P_PARTKEY), MAX(LINEITEM.L_RETURNFLAG) FROM PART JOIN PARTSUPP ON PARTSUPP.PS_PARTKEY=PART.P_PARTKEY JOIN LINEITEM ON LINEITEM.L_SUPPKEY=PARTSUPP.PS_SUPPKEY GROUP BY LINEITEM.L_SUPPKEY, PARTSUPP.PS_SUPPLYCOST, PART.P_PARTKEY HAVING MAX(LINEITEM.L_RETURNFLAG) >= 'N' AND MAX(PART.P_PARTKEY) > 30;