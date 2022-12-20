SELECT PARTSUPP.PS_SUPPLYCOST, COUNT(PARTSUPP.PS_AVAILQTY) FROM PARTSUPP GROUP BY PARTSUPP.PS_SUPPLYCOST HAVING COUNT(PARTSUPP.PS_AVAILQTY) != 8 ORDER BY PARTSUPP.PS_SUPPLYCOST DESC;