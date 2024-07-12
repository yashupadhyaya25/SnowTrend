/*
################################################################
# Test Sql Created On - 2024/07/11 by Yash Upadhyaya
# Added Test on 2024/07/11 by Yash Upadhyaya
# - Check Price of all stock are greater than 0
################################################################
*/

SELECT * FROM RAW.STOCK_DATA.ALL_STOCK
WHERE
LOW <= 0 or
HIGH <= 0 or 
OPEN <= 0 or 
CLOSE <= 0
