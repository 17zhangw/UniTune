SELECT ORDERS.O_COMMENT, CUSTOMER.C_NATIONKEY, MIN(CUSTOMER.C_COMMENT) FROM ORDERS JOIN CUSTOMER ON CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY GROUP BY ORDERS.O_COMMENT, CUSTOMER.C_NATIONKEY HAVING MIN(CUSTOMER.C_COMMENT) > 'THELY FINAL IDEAS AROUND THE QUICKLY FINAL DEPENDENCIES AFFIX CAREFULLY QUICKLY FINAL THEODOLITES. FINAL ACCOUNTS C';