select product_id, round(sum(sales_amount),2) as Total_Sales
from fact_sales
group by product_id;