SELECT PART.P_RETAILPRICE, PARTSUPP.PS_PARTKEY FROM PARTSUPP, PART WHERE PART.P_MFGR > 'MANUFACTURER#3           ' AND PART.P_SIZE != 35;