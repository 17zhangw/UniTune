SELECT CUSTOMER.C_COMMENT, COUNT(CUSTOMER.C_PHONE) FROM CUSTOMER GROUP BY CUSTOMER.C_COMMENT HAVING COUNT(CUSTOMER.C_PHONE) = 1 ORDER BY COUNT(CUSTOMER.C_PHONE) DESC;