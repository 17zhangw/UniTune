SELECT LINEITEM.L_COMMITDATE, PART.P_COMMENT, PARTSUPP.PS_COMMENT FROM LINEITEM JOIN PARTSUPP ON PARTSUPP.PS_SUPPKEY=LINEITEM.L_SUPPKEY JOIN PART ON PART.P_PARTKEY=PARTSUPP.PS_PARTKEY WHERE LINEITEM.L_PARTKEY < 6774 ORDER BY LINEITEM.L_COMMITDATE DESC, PARTSUPP.PS_COMMENT ASC;