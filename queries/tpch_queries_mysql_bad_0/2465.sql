SELECT SUPPLIER.S_COMMENT, PARTSUPP.PS_AVAILQTY FROM SUPPLIER, PARTSUPP WHERE SUPPLIER.S_ACCTBAL != 1365.79 ORDER BY SUPPLIER.S_COMMENT ASC, PARTSUPP.PS_AVAILQTY ASC;