SELECT LINEITEM.L_LINENUMBER, PARTSUPP.PS_COMMENT, MAX(PARTSUPP.PS_PARTKEY) FROM PARTSUPP, LINEITEM GROUP BY LINEITEM.L_LINENUMBER, PARTSUPP.PS_COMMENT;