SELECT ORDERS.O_CLERK, CUSTOMER.C_COMMENT FROM ORDERS JOIN CUSTOMER ON CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY WHERE CUSTOMER.C_ADDRESS >= '9PWKUHZT4ZR1Q' ORDER BY ORDERS.O_CLERK DESC;