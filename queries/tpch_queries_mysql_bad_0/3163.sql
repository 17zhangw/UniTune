SELECT NATION.N_NATIONKEY, REGION.R_REGIONKEY FROM REGION JOIN NATION ON NATION.N_REGIONKEY=REGION.R_REGIONKEY ORDER BY NATION.N_NATIONKEY ASC, REGION.R_REGIONKEY ASC;