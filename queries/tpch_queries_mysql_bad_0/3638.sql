SELECT PARTSUPP.PS_PARTKEY, LINEITEM.L_COMMITDATE FROM LINEITEM JOIN PARTSUPP ON PARTSUPP.PS_SUPPKEY=LINEITEM.L_SUPPKEY ORDER BY LINEITEM.L_COMMITDATE DESC;