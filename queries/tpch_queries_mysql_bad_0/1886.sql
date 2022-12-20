SELECT ORDERS.O_CLERK, CUSTOMER.C_MKTSEGMENT, LINEITEM.L_COMMITDATE, SUPPLIER.S_NAME, NATION.N_REGIONKEY FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY JOIN CUSTOMER ON CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY JOIN ORDERS ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY JOIN LINEITEM ON LINEITEM.L_ORDERKEY=ORDERS.O_ORDERKEY WHERE ORDERS.O_SHIPPRIORITY = 0;