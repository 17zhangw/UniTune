SELECT PART.P_SIZE, PARTSUPP.PS_AVAILQTY FROM PARTSUPP JOIN PART ON PART.P_PARTKEY=PARTSUPP.PS_PARTKEY ORDER BY PARTSUPP.PS_AVAILQTY DESC, PART.P_SIZE ASC;