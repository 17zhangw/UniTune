SELECT REGION.R_COMMENT, NATION.N_REGIONKEY FROM NATION JOIN REGION ON REGION.R_REGIONKEY=NATION.N_REGIONKEY WHERE NATION.N_NATIONKEY != 227437.30 ORDER BY NATION.N_REGIONKEY DESC;