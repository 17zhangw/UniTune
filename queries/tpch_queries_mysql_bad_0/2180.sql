SELECT REGION.R_NAME, NATION.N_NAME FROM REGION JOIN NATION ON NATION.N_REGIONKEY=REGION.R_REGIONKEY ORDER BY NATION.N_NAME ASC;