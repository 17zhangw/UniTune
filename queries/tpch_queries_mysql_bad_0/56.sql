SELECT PARTSUPP.PS_PARTKEY, LINEITEM.L_COMMITDATE FROM PARTSUPP JOIN LINEITEM ON LINEITEM.L_SUPPKEY=PARTSUPP.PS_SUPPKEY;