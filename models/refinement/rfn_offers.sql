SELECT ARRAY_AGG (OFFERID) AS OFFERID, ASIN FROM BUYBOX.RAW.SRC_OFFERS WHERE ISBUYBOXWINNER = TRUE GROUP BY ASIN 