SELECT LINEITEM.L_SHIPDATE, PARTSUPP.PS_SUPPKEY FROM PARTSUPP JOIN LINEITEM ON LINEITEM.L_SUPPKEY=PARTSUPP.PS_SUPPKEY ORDER BY PARTSUPP.PS_SUPPKEY ASC;