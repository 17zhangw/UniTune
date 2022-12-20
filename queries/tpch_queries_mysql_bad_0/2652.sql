SELECT SUPPLIER.S_ADDRESS, NATION.N_NATIONKEY, REGION.R_REGIONKEY FROM REGION JOIN NATION ON NATION.N_REGIONKEY=REGION.R_REGIONKEY JOIN SUPPLIER ON SUPPLIER.S_NATIONKEY=NATION.N_NATIONKEY WHERE REGION.R_REGIONKEY > 82702.94 ORDER BY SUPPLIER.S_ADDRESS DESC, REGION.R_REGIONKEY ASC;