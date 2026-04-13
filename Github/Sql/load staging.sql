alter table staging.sales
alter column salesid type text,
alter column salespersonid type text,
alter column customerid type text,
alter column productid type text,
alter column quantity type text,
alter column discount type text,
alter column totalprice type text,
alter column salesdate type text;

alter table staging.sales
alter column salesid type integer using nullif(trim(salesid), '')::integer,
alter column salespersonid type integer using nullif(trim(salespersonid),'')::integer,
alter column customerid type integer using nullif(trim(customerid), '')::integer,
alter column productid type integer using nullif(trim(productid), '')::integer,
alter column quantity type integer using nullif(trim(quantity), '')::integer,
alter column discount type numeric(6,2) using nullif(trim(discount), '')::numeric(6,2),
alter column totalprice type numeric(18,2) using nullif(trim(totalprice), '')::numeric(18,2),
alter column salesdate type date using nullif(trim(salesdate), '')::date;