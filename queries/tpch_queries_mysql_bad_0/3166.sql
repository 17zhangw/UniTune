SELECT SUPPLIER.S_NATIONKEY, MIN(SUPPLIER.S_PHONE) FROM SUPPLIER GROUP BY SUPPLIER.S_NATIONKEY;