SELECT PARTSUPP.PS_SUPPKEY, COUNT(PARTSUPP.PS_SUPPKEY) FROM PARTSUPP GROUP BY PARTSUPP.PS_SUPPKEY;