SELECT PART.P_CONTAINER, PARTSUPP.PS_COMMENT FROM PARTSUPP JOIN PART ON PART.P_PARTKEY=PARTSUPP.PS_PARTKEY ORDER BY PARTSUPP.PS_COMMENT ASC;