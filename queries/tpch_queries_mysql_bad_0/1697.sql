SELECT SUPPLIER.S_ACCTBAL, NATION.N_NATIONKEY, REGION.R_COMMENT FROM SUPPLIER JOIN NATION ON NATION.N_NATIONKEY=SUPPLIER.S_NATIONKEY JOIN REGION ON REGION.R_REGIONKEY=NATION.N_REGIONKEY;