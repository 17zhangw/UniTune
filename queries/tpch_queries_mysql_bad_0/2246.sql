SELECT PARTSUPP.PS_SUPPKEY, COUNT(PARTSUPP.PS_SUPPKEY) FROM PARTSUPP WHERE PARTSUPP.PS_SUPPLYCOST >= 524.27 GROUP BY PARTSUPP.PS_SUPPKEY;