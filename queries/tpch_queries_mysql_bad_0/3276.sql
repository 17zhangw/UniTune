SELECT LINEITEM.L_RECEIPTDATE, SUPPLIER.S_NATIONKEY, PARTSUPP.PS_COMMENT, COUNT(LINEITEM.L_RECEIPTDATE) FROM SUPPLIER JOIN PARTSUPP ON PARTSUPP.PS_SUPPKEY=SUPPLIER.S_SUPPKEY JOIN LINEITEM ON LINEITEM.L_SUPPKEY=PARTSUPP.PS_SUPPKEY GROUP BY LINEITEM.L_RECEIPTDATE, SUPPLIER.S_NATIONKEY, PARTSUPP.PS_COMMENT ORDER BY LINEITEM.L_RECEIPTDATE ASC;