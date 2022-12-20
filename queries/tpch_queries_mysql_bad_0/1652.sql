SELECT PARTSUPP.PS_SUPPKEY, LINEITEM.L_COMMENT, MIN(PARTSUPP.PS_AVAILQTY) FROM PARTSUPP JOIN LINEITEM ON LINEITEM.L_SUPPKEY=PARTSUPP.PS_SUPPKEY WHERE LINEITEM.L_LINESTATUS != 'F' AND LINEITEM.L_SHIPMODE != 'RAIL      ' GROUP BY PARTSUPP.PS_SUPPKEY, LINEITEM.L_COMMENT;