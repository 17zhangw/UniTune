SELECT PART.P_SIZE, NATION.N_REGIONKEY, SUPPLIER.S_ACCTBAL, PARTSUPP.PS_SUPPKEY, REGION.R_REGIONKEY FROM REGION, NATION, SUPPLIER, PARTSUPP, PART ORDER BY NATION.N_REGIONKEY DESC, REGION.R_REGIONKEY DESC, PART.P_SIZE ASC;