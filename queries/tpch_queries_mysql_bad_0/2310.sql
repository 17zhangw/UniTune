SELECT LINEITEM.L_DISCOUNT, SUPPLIER.S_SUPPKEY, PARTSUPP.PS_AVAILQTY FROM LINEITEM, PARTSUPP, SUPPLIER ORDER BY LINEITEM.L_DISCOUNT DESC, PARTSUPP.PS_AVAILQTY DESC, SUPPLIER.S_SUPPKEY DESC;