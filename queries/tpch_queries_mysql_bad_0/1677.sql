SELECT PART.P_MFGR, COUNT(PART.P_BRAND) FROM PART GROUP BY PART.P_MFGR ORDER BY COUNT(PART.P_BRAND) DESC;