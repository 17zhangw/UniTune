SELECT PART.P_BRAND, AVG(PART.P_SIZE) FROM PART WHERE PART.P_CONTAINER != 'WRAP CASE ' AND PART.P_RETAILPRICE > 1042.14 AND PART.P_MFGR > 'MANUFACTURER#2           ' GROUP BY PART.P_BRAND;