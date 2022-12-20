SELECT CUSTOMER.C_NATIONKEY, ORDERS.O_COMMENT, MAX(CUSTOMER.C_COMMENT) FROM ORDERS JOIN CUSTOMER ON CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY GROUP BY CUSTOMER.C_NATIONKEY, ORDERS.O_COMMENT HAVING MAX(CUSTOMER.C_COMMENT) > 'RONIC IDEAS USE ABOVE THE SLOWLY PENDIN' ORDER BY MAX(CUSTOMER.C_COMMENT) DESC;