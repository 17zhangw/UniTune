SELECT LINEITEM.L_RECEIPTDATE, PARTSUPP.PS_AVAILQTY, MAX(PARTSUPP.PS_SUPPLYCOST) FROM PARTSUPP, LINEITEM GROUP BY LINEITEM.L_RECEIPTDATE, PARTSUPP.PS_AVAILQTY;