SELECT PARTSUPP.PS_AVAILQTY, PART.P_BRAND FROM PARTSUPP JOIN PART ON PART.P_PARTKEY=PARTSUPP.PS_PARTKEY ORDER BY PART.P_BRAND DESC, PARTSUPP.PS_AVAILQTY ASC;