SELECT PARTSUPP.PS_SUPPLYCOST, PART.P_NAME FROM PARTSUPP JOIN PART ON PART.P_PARTKEY=PARTSUPP.PS_PARTKEY ORDER BY PART.P_NAME ASC, PARTSUPP.PS_SUPPLYCOST ASC;