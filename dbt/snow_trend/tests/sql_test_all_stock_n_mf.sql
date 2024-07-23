/*
################################################################
# Test Sql Created On - 2024/07/11 by Yash Upadhyaya
# Added Test on 2024/07/11 by Yash Upadhyaya
# - Check Price of all stock are greater than 0
################################################################
*/

SELECT * FROM STOCK_RAW.ALL_STOCK
WHERE
LOW <= 0 or
HIGH <= 0 or 
OPEN <= 0 or 
CLOSE <= 0
UNION
SELECT * FROM STOCK_RAW.ALL_MF
WHERE
LOW <= 0 or
HIGH <= 0 or 
OPEN <= 0 or 
CLOSE <= 0
