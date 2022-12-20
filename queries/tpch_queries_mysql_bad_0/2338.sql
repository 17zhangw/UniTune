SELECT PART.P_NAME, LINEITEM.L_RECEIPTDATE, PARTSUPP.PS_PARTKEY, MIN(LINEITEM.L_RETURNFLAG) FROM LINEITEM, PARTSUPP, PART GROUP BY PART.P_NAME, LINEITEM.L_RECEIPTDATE, PARTSUPP.PS_PARTKEY HAVING MIN(LINEITEM.L_RETURNFLAG) <= 'A';