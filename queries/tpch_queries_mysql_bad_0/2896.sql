SELECT NATION.N_REGIONKEY, REGION.R_COMMENT FROM REGION JOIN NATION ON NATION.N_REGIONKEY=REGION.R_REGIONKEY ORDER BY REGION.R_COMMENT ASC, NATION.N_REGIONKEY DESC;