select * from fact_sales where sales_amount > 200 and quantity>=3; 
select product_id, round(sum(sales_amount),2) as Total_Sales from fact_sales group by product_id;
select customer_id, round(sum(sales_amount),2) as Total_Sales from fact_sales group by customer_id having Total_Sales>15000;
select c.country, round(sum(s.sales_amount),2)  as Total_Amount from dim_customer c inner join fact_sales s on s.customer_id=c.customer_id group by c.country;
select customer_id from fact_sales where sales_amount > ( select avg(sales_amount) from fact_sales )