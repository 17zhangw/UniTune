SELECT REGION.R_NAME, AVG(REGION.R_REGIONKEY), COUNT(REGION.R_REGIONKEY) FROM REGION GROUP BY REGION.R_NAME ORDER BY AVG(REGION.R_REGIONKEY) ASC, COUNT(REGION.R_REGIONKEY) DESC;