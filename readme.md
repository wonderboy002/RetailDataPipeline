Invoice, ---------- 
DateID,           |
StockCode,        |
Customer ID       |   -> Fact Sales
Price,            |
SalesAmount,      |
Quantity ---------|

DateID, full_date, year, month, day, Quarter   (dim date)

StockCode, Description (dim Product)

Customer ID, Country (dim Customer)