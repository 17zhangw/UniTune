SELECT NATION.N_NATIONKEY, REGION.R_COMMENT FROM REGION, NATION WHERE NATION.N_NAME < '28058.15' AND NATION.N_COMMENT != '19411' ORDER BY REGION.R_COMMENT DESC, NATION.N_NATIONKEY ASC;