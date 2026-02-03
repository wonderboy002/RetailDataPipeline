select customer_id, round(sum(sales_amount),2) as Total_Sales
from fact_sales
group by customer_id
having Total_Sales>15000;