SELECT LINEITEM.L_ORDERKEY, MAX(LINEITEM.L_EXTENDEDPRICE) FROM LINEITEM GROUP BY LINEITEM.L_ORDERKEY HAVING MAX(LINEITEM.L_EXTENDEDPRICE) < 25156.46;