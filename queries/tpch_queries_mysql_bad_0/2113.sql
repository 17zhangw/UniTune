SELECT LINEITEM.L_RETURNFLAG, ORDERS.O_CLERK FROM LINEITEM JOIN ORDERS ON ORDERS.O_ORDERKEY=LINEITEM.L_ORDERKEY ORDER BY ORDERS.O_CLERK DESC, LINEITEM.L_RETURNFLAG DESC;