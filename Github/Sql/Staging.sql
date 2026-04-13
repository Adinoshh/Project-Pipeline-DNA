create schema staging;
create schema dimension;

create table staging.sales( 
salesid int,
salespersonid int,
customerid int,
productid int,
quantity int,
discount numeric(6,2),
totalprice numeric(18,2),
salesdate date,
transactionnumber text
);


create table staging.categories(
categoryid int,
categoryname text
);

create table staging.cities(
cityid int,
cityname text,
zipcode text,
countryid int
);

create table staging.countries(
countryid int,
countryname text,
countrycode text
);

create table staging.customer(
customerid int,
firstname text,
middleinitial text,
lastname text,
cityid int,
address text
);

create table staging.employee(
employeeid int,
firstname text,
middleinitial text,
lastname text,
birthdate date,
gender text,
cityid int,
hiredate timestamp
);

create table staging.products(
productid int,
productname text,
price numeric(18,2),
categoryid int,
class text,
modifydate timestamp,
resistant text,
isallergic text,
vitalitydays text
);