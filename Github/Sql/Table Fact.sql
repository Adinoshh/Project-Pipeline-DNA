CREATE TABLE dimension.fact_sales(
sk_date int,
sk_customer int,
sk_employee int,
sk_product int,
sales_id int,
transaction_no text,
quantity decimal(18,2),
discount decimal(6,2),
total_price decimal(18,2),
insert_date date
);

insert into dimension.fact_sales(
sk_date,
sk_customer,
sk_employee,
sk_product,
sales_id,
transaction_no,
quantity,
discount,
total_price,
insert_date
)
select
dt.sk_date,
dc.sk_customer,
de.sk_employee,
dp.sk_product,
s.salesid,
s.transactionnumber,
s.quantity,
s.discount,
s.totalprice,
current_date
from staging.sales s
left join dimension.dim_date dt
on s.salesdate = dt.date
left join dimension.dim_customer dc 
on s.customerid = dc.customer_id
left join dimension.dim_employee de
on s.salespersonid = de.employee_id
left join dimension.dim_product dp 
on s.productid = dp.product_id;