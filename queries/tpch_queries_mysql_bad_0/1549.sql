SELECT LINEITEM.L_SUPPKEY, PART.P_MFGR, ORDERS.O_TOTALPRICE, PARTSUPP.PS_PARTKEY FROM ORDERS, LINEITEM, PARTSUPP, PART;