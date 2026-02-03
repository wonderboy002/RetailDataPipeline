select c.country, round(sum(s.sales_amount),2)  as Total_Amount
from dim_customer c inner join fact_sales s
on s.customer_id=c.customer_id
group by c.country;