SELECT LINEITEM.L_QUANTITY, MIN(LINEITEM.L_SHIPINSTRUCT), MIN(LINEITEM.L_SUPPKEY) FROM LINEITEM WHERE LINEITEM.L_TAX = 0.00 GROUP BY LINEITEM.L_QUANTITY;