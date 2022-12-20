SELECT SUPPLIER.S_SUPPKEY, REGION.R_COMMENT, NATION.N_NATIONKEY FROM SUPPLIER, NATION, REGION WHERE REGION.R_NAME <= '267' AND REGION.R_NAME > '13961' ORDER BY REGION.R_COMMENT ASC, SUPPLIER.S_SUPPKEY DESC, NATION.N_NATIONKEY ASC;