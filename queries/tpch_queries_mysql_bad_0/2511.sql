SELECT LINEITEM.L_EXTENDEDPRICE, MIN(LINEITEM.L_SHIPDATE) FROM LINEITEM GROUP BY LINEITEM.L_EXTENDEDPRICE HAVING MIN(LINEITEM.L_SHIPDATE) >= '1996-03-18' ORDER BY MIN(LINEITEM.L_SHIPDATE) ASC;