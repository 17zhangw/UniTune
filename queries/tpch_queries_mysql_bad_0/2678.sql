SELECT NATION.N_REGIONKEY, REGION.R_NAME FROM NATION JOIN REGION ON REGION.R_REGIONKEY=NATION.N_REGIONKEY WHERE NATION.N_NAME = 'GGLE ABOUT THE FURIOUSLY R' ORDER BY NATION.N_REGIONKEY ASC;