SELECT LINEITEM.L_LINENUMBER, ORDERS.O_CUSTKEY, REGION.R_REGIONKEY, CUSTOMER.C_ADDRESS, PARTSUPP.PS_AVAILQTY, NATION.N_NATIONKEY FROM PARTSUPP, LINEITEM, ORDERS, CUSTOMER, NATION, REGION WHERE CUSTOMER.C_ADDRESS < 'HNHTNB5XPNSF20JBH4YCS6PSVNKC3RDF' ORDER BY REGION.R_REGIONKEY DESC, CUSTOMER.C_ADDRESS ASC, LINEITEM.L_LINENUMBER ASC, ORDERS.O_CUSTKEY ASC, PARTSUPP.PS_AVAILQTY ASC, NATION.N_NATIONKEY DESC;