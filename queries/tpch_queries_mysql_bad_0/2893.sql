SELECT PART.P_CONTAINER, LINEITEM.L_RECEIPTDATE, PARTSUPP.PS_AVAILQTY FROM PART JOIN PARTSUPP ON PARTSUPP.PS_PARTKEY=PART.P_PARTKEY JOIN LINEITEM ON LINEITEM.L_SUPPKEY=PARTSUPP.PS_SUPPKEY;