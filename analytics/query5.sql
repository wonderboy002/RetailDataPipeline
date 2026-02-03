select customer_id
from fact_sales 
where sales_amount > (
    select avg(sales_amount) from fact_sales
)