SELECT CUSTOMER.C_NATIONKEY, NATION.N_NAME, REGION.R_REGIONKEY FROM REGION JOIN NATION ON NATION.N_REGIONKEY=REGION.R_REGIONKEY JOIN CUSTOMER ON CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY WHERE NATION.N_NATIONKEY != 1993-12-25;