SELECT PART.P_RETAILPRICE, PARTSUPP.PS_AVAILQTY, SUPPLIER.S_SUPPKEY FROM PART JOIN PARTSUPP ON PARTSUPP.PS_PARTKEY=PART.P_PARTKEY JOIN SUPPLIER ON SUPPLIER.S_SUPPKEY=PARTSUPP.PS_SUPPKEY WHERE PARTSUPP.PS_PARTKEY < 155 ORDER BY SUPPLIER.S_SUPPKEY ASC, PART.P_RETAILPRICE ASC;