SELECT ORDERS.O_ORDERSTATUS, CUSTOMER.C_COMMENT FROM CUSTOMER JOIN ORDERS ON ORDERS.O_CUSTKEY=CUSTOMER.C_CUSTKEY WHERE CUSTOMER.C_ADDRESS != 'M1ETOIECUVH8DTM0Y0NRYXFW';