SELECT NATION.N_COMMENT, COUNT(NATION.N_REGIONKEY) FROM NATION GROUP BY NATION.N_COMMENT ORDER BY NATION.N_COMMENT ASC;