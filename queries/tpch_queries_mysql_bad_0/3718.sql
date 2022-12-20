SELECT LINEITEM.L_RECEIPTDATE, NATION.N_NAME, CUSTOMER.C_ADDRESS, ORDERS.O_CLERK, MIN(NATION.N_NATIONKEY), MAX(LINEITEM.L_RECEIPTDATE) FROM LINEITEM, ORDERS, CUSTOMER, NATION GROUP BY LINEITEM.L_RECEIPTDATE, NATION.N_NAME, CUSTOMER.C_ADDRESS, ORDERS.O_CLERK HAVING MIN(NATION.N_NATIONKEY) = PART ORDER BY MIN(NATION.N_NATIONKEY) ASC;