SELECT SUPPLIER.S_ADDRESS, PARTSUPP.PS_AVAILQTY, MAX(SUPPLIER.S_NAME), COUNT(PARTSUPP.PS_COMMENT) FROM PARTSUPP, SUPPLIER WHERE SUPPLIER.S_PHONE < '15-679-861-2259' GROUP BY SUPPLIER.S_ADDRESS, PARTSUPP.PS_AVAILQTY;