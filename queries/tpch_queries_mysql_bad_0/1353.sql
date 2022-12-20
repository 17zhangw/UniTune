SELECT CUSTOMER.C_NATIONKEY, NATION.N_NATIONKEY, REGION.R_COMMENT FROM CUSTOMER JOIN NATION ON NATION.N_NATIONKEY=CUSTOMER.C_NATIONKEY JOIN REGION ON REGION.R_REGIONKEY=NATION.N_REGIONKEY WHERE CUSTOMER.C_PHONE != '28-183-750-7809' AND CUSTOMER.C_ACCTBAL > 5897.83 ORDER BY CUSTOMER.C_NATIONKEY ASC;