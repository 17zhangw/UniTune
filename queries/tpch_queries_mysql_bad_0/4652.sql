SELECT PARTSUPP.PS_PARTKEY, SUM(PARTSUPP.PS_AVAILQTY) FROM PARTSUPP GROUP BY PARTSUPP.PS_PARTKEY ORDER BY SUM(PARTSUPP.PS_AVAILQTY) DESC;