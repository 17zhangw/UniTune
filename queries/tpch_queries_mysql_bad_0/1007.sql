SELECT LINEITEM.L_TAX, CUSTOMER.C_ADDRESS, NATION.N_NATIONKEY, ORDERS.O_TOTALPRICE, REGION.R_REGIONKEY FROM LINEITEM JOIN ORDERS ON ORDERS.O_ORDERKEY=LINEITEM.L_ORDERKEY JOIN CUSTOMER ON CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY JOIN NATION ON NATION.N_NATIONKEY=CUSTOMER.C_NATIONKEY JOIN REGION ON REGION.R_REGIONKEY=NATION.N_REGIONKEY WHERE LINEITEM.L_RECEIPTDATE = '1998-07-09' AND CUSTOMER.C_CUSTKEY > 10 ORDER BY CUSTOMER.C_ADDRESS DESC;