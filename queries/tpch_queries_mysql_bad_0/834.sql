SELECT PARTSUPP.PS_AVAILQTY, MIN(PARTSUPP.PS_PARTKEY) FROM PARTSUPP WHERE PARTSUPP.PS_COMMENT != 'DEPENDENCIES. SPECIAL ACCOUNTS WAKE CAREFULLY FURIOUSLY REGULAR ACCOUNTS. REGULAR ACCOUNTS HAGGLE ALONG THE EXPRESS INSTRUCTIONS. EXPRESS PINTO BEANS ALONG THE EXPRESS, BOLD DEPOSITS RUN ' AND PARTSUPP.PS_SUPPKEY != 617 GROUP BY PARTSUPP.PS_AVAILQTY HAVING MIN(PARTSUPP.PS_PARTKEY) >= 51 ORDER BY PARTSUPP.PS_AVAILQTY ASC;