SELECT SUPPLIER.S_SUPPKEY, REGION.R_REGIONKEY, NATION.N_COMMENT FROM REGION JOIN NATION ON NATION.N_REGIONKEY=REGION.R_REGIONKEY JOIN SUPPLIER ON SUPPLIER.S_NATIONKEY=NATION.N_NATIONKEY WHERE SUPPLIER.S_COMMENT != ' SLYLY BOLD INSTRUCTIONS. IDLE DEPENDEN' ORDER BY SUPPLIER.S_SUPPKEY ASC;