SELECT SUPPLIER.S_PHONE, MIN(SUPPLIER.S_ADDRESS), COUNT(SUPPLIER.S_COMMENT), SUM(SUPPLIER.S_NATIONKEY) FROM SUPPLIER GROUP BY SUPPLIER.S_PHONE ORDER BY SUPPLIER.S_PHONE ASC;