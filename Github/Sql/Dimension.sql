create table dimension.dim_product(
sk_product serial primary key,
product_id int,
product_name text,
product_price decimal(18,2),
category_id int,
category_name text,
modify_datetime timestamp,
insert_date date
);

insert into dimension.dim_product(
product_id,
product_name,
product_price,
category_id,
category_name,
modify_datetime,
insert_date
)
select distinct
p.productid,
p.productname,
p.price,
p.categoryid,
c.categoryname,
p.modifydate,
current_date
from staging.products p 
left join staging.categories c 
on p.categoryid = c.categoryid;

select * from dimension.dim_product;

create table dimension.dim_customer(
sk_customer int generated always as identity primary key,
customer_id int,
customer_first_name text,
customer_middle_initial_name text,
customer_last_name text,
customer_address text,
customer_city_id int,
customer_city_name text,
customer_zipcode text,
customer_country_id int,
customer_country_name text,
customer_country_code text,
start_date date,
end_date date
);

select * from dimension.dim_customer;

insert into dimension.dim_customer(
customer_id,
customer_first_name,
customer_middle_initial_name,
customer_last_name,
customer_address,
customer_city_id,
customer_city_name,
customer_zipcode,
customer_country_id,
customer_country_name,
customer_country_code,
start_date,
end_date
)
select distinct
cs.customerid,
cs.firstname,
cs.middleinitial,
cs.lastname,
cs.address,
cs.cityid,
ci.cityname,
ci.zipcode,
co.countryid,
co.countryname,
co.countrycode,
current_date,
null::date
from staging.customer cs
left join staging.cities ci
on cs.cityid = ci.cityid
left join staging.countries co
on ci.countryid = co.countryid;

create table dimension.dim_employee(
sk_employee int generated always as identity primary key,
employee_id int,
employee_first_name text,
employee_middle_initial text,
employee_last_name text,
employee_birth_date date,
employee_gender text,
employee_city_id int,
employee_city_name text,
employee_country_id int,
employee_country_name text,
employee_country_code text,
employee_hire_date timestamp,
start_date date,
end_date date
);

select * from dimension.dim_employee e;

insert into dimension.dim_employee(
employee_id,
employee_first_name,
employee_middle_initial,
employee_last_name,
employee_birth_date,
employee_gender,
employee_city_id,
employee_city_name,
employee_country_id,
employee_country_name,
employee_country_code,
employee_hire_date,
start_date,
end_date
)
select distinct
e.employeeid,
e.firstname,
e.middleinitial,
e.lastname,
e.birthdate,
e.gender,
e.cityid,
ci.cityname,
co.countryid,
co.countryname,
co.countrycode,
e.hiredate,
current_date,
null::date
from staging.employee e
left join staging.cities ci
on e.cityid = ci.cityid
left join staging.countries co
on ci.countryid = co.countryid;

CREATE TABLE dimension.dim_time (
    sk_date SERIAL PRIMARY KEY, -- SERIAL is an integer that will auto-increment as new rows added
    date DATE,
    days VARCHAR(40),
    month_id INTEGER,
    month_name VARCHAR(40),
    year INTEGER
);

select * from dimension.dim_time;

INSERT INTO dimension.dim_date (date, days, month_id, month_name, year)
SELECT 
days.d::DATE as date, 
to_char(days.d, 'FMMonth DD, YYYY') as days, 
to_char(days.d, 'MM')::integer as month_id, 
to_char(days.d, 'FMMonth') as month_name, 
to_char(days.d, 'YYYY')::integer as year
from (
    SELECT generate_series(
        ('2000-01-01')::date, -- 'start' date
        ('2100-12-31')::date, -- 'end' date
        interval '1 day'  -- one for each day between the start and day
        )) as days(d);
