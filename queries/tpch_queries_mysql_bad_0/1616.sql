SELECT REGION.R_REGIONKEY, NATION.N_NATIONKEY FROM NATION JOIN REGION ON REGION.R_REGIONKEY=NATION.N_REGIONKEY ORDER BY NATION.N_NATIONKEY DESC, REGION.R_REGIONKEY ASC;