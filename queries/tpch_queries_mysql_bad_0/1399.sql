SELECT SUPPLIER.S_ACCTBAL, REGION.R_NAME, NATION.N_NAME FROM REGION JOIN NATION ON NATION.N_REGIONKEY=REGION.R_REGIONKEY JOIN SUPPLIER ON SUPPLIER.S_NATIONKEY=NATION.N_NATIONKEY ORDER BY NATION.N_NAME ASC, REGION.R_NAME DESC;