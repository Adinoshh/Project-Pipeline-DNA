from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


# Load Staging
def load_staging():
    conn = psycopg2.connect(
        host="ep-crimson-cherry-ai2u68j1-pooler.c-4.us-east-1.aws.neon.tech",
        port="5432",
        dbname="neondb",
        user="neondb_owner",
        password="-",
        sslmode="require"
    )
    cur = conn.cursor()

    # LOAD SALES
    sales1 = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.sales.csv", sep=";")
    sales2 = pd.read_csv("/opt/airflow/dags/input/20180509/20180509.sales.csv", sep=";")
    sales = pd.concat([sales1, sales2], ignore_index=True)

    sales.columns = sales.columns.str.strip().str.lower()
    sales["salesdate"] = pd.to_datetime(sales["salesdate"], format="%Y%m%d", errors="coerce").dt.date
    sales["discount"] = sales["discount"].fillna(0)
    sales["totalprice"] = sales["totalprice"].fillna(0)

    cur.execute("TRUNCATE TABLE staging.sales;")

    execute_batch(cur, """
        INSERT INTO staging.sales (
            salesid, salespersonid, customerid, productid,
            quantity, discount, totalprice, salesdate, transactionnumber
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, sales[[
        "salesid", "salespersonid", "customerid", "productid",
        "quantity", "discount", "totalprice", "salesdate", "transactionnumber"
    ]].values.tolist())

    # =====================
    # LOAD PRODUCTS
    # =====================
    products = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.products.csv", sep=";")
    products.columns = products.columns.str.strip().str.lower()
    products["modifydate"] = pd.to_datetime(products["modifydate"], errors="coerce")
    products["price"] = products["price"].astype(str).str.replace(",", ".", regex=False)
    products["price"] = pd.to_numeric(products["price"], errors="coerce").fillna(0)

    cur.execute("TRUNCATE TABLE staging.products;")

    execute_batch(cur, """
        INSERT INTO staging.products (
            productid, productname, price, categoryid,
            class, modifydate, resistant, isallergic, vitalitydays
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, products[[
        "productid", "productname", "price", "categoryid",
        "class", "modifydate", "resistant", "isallergic", "vitalitydays"
    ]].values.tolist())

    # =====================
    # LOAD CUSTOMERS
    # =====================
    customers = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.customers.csv", sep=";")
    customers.columns = customers.columns.str.strip().str.lower()

    cur.execute("TRUNCATE TABLE staging.customer;")

    execute_batch(cur, """
        INSERT INTO staging.customer (
            customerid, firstname, middleinitial, lastname, cityid, address
        )
        VALUES (%s,%s,%s,%s,%s,%s)
    """, customers[[
        "customerid", "firstname", "middleinitial", "lastname", "cityid", "address"
    ]].values.tolist())

    # =====================
    # LOAD CATEGORIES
    # =====================
    categories = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.categories.csv", sep="|")
    categories.columns = categories.columns.str.strip().str.lower()

    cur.execute("TRUNCATE TABLE staging.categories;")

    execute_batch(cur, """
        INSERT INTO staging.categories (
            categoryid, categoryname
        )
        VALUES (%s,%s)
    """, categories[[
        "categoryid", "categoryname"
    ]].values.tolist())

    # =====================
    # LOAD EMPLOYEE
    # =====================
    employee = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.employes.csv", sep=";")
    employee.columns = employee.columns.str.strip().str.lower()
    employee["birthdate"] = pd.to_datetime(employee["birthdate"], errors="coerce")
    employee["hiredate"] = pd.to_datetime(employee["hiredate"], errors="coerce")

    cur.execute("TRUNCATE TABLE staging.employee;")

    execute_batch(cur, """
        INSERT INTO staging.employee (
            employeeid, firstname, middleinitial, lastname,
            birthdate, gender, cityid, hiredate
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, employee[[
        "employeeid", "firstname", "middleinitial", "lastname",
        "birthdate", "gender", "cityid", "hiredate"
    ]].values.tolist())

    # =====================
    # LOAD CITIES
    # =====================
    cities = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.cities.csv", sep=";")
    cities.columns = cities.columns.str.strip().str.lower()

    cur.execute("TRUNCATE TABLE staging.cities;")

    execute_batch(cur, """
        INSERT INTO staging.cities (
            cityid, cityname, zipcode, countryid
        )
        VALUES (%s,%s,%s,%s)
    """, cities[[
        "cityid", "cityname", "zipcode", "countryid"
    ]].values.tolist())

    # =====================
    # LOAD COUNTRIES
    # =====================
    countries = pd.read_csv("/opt/airflow/dags/input/20180508/20180508.countries.csv", sep=";")
    countries.columns = countries.columns.str.strip().str.lower()

    cur.execute("TRUNCATE TABLE staging.countries;")

    execute_batch(cur, """
        INSERT INTO staging.countries (
            countryid, countryname, countrycode
        )
        VALUES (%s,%s,%s)
    """, countries[[
        "countryid", "countryname", "countrycode"
    ]].values.tolist())

    conn.commit()
    cur.close()
    conn.close()

    print("Load staging selesai")


# =========================
# DAG AIRFLOW
# =========================
with DAG(
    dag_id="giffari_pipeline_sederhana",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual run
    catchup=False,
    tags=["mini_project", "etl", "neondb"]
) as dag:

    # 1. load csv ke staging
    task_load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging
    )

    # 2. buat / isi dim_time
    task_dim_time = SQLExecuteQueryOperator(
        task_id="dim_time",
        conn_id="giffari_db",
        sql="""
        INSERT INTO dimension.dim_time (date, days, month_id, month_name, year)
        SELECT
            d::date,
            to_char(d, 'FMMonth DD, YYYY'),
            to_char(d, 'MM')::int,
            to_char(d, 'FMMonth'),
            to_char(d, 'YYYY')::int
        FROM generate_series('2000-01-01'::date, '2100-12-31'::date, interval '1 day') AS d
        WHERE NOT EXISTS (
            SELECT 1
            FROM dimension.dim_time t
            WHERE t.date = d::date
        );
        """
    )

    # 3. dim_product
    task_dim_product = SQLExecuteQueryOperator(
        task_id="dim_product",
        conn_id="giffari_db",
        sql="""
        INSERT INTO dimension.dim_product (
            product_id, product_name, product_price, category_id, category_name, modify_datetime
        )
        SELECT
            p.productid,
            p.productname,
            p.price,
            p.categoryid,
            c.categoryname,
            p.modifydate
        FROM staging.products p
        LEFT JOIN staging.categories c
            ON p.categoryid = c.categoryid
        WHERE NOT EXISTS (
            SELECT 1
            FROM dimension.dim_product dp
            WHERE dp.product_id = p.productid
        );
        """
    )

    # 4. dim_customer
    task_dim_customer = SQLExecuteQueryOperator(
        task_id="dim_customer",
        conn_id="giffari_db",
        sql="""
        INSERT INTO dimension.dim_customer (
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
        SELECT
            cu.customerid,
            cu.firstname,
            cu.middleinitial,
            cu.lastname,
            cu.address,
            cu.cityid,
            ci.cityname,
            ci.zipcode,
            co.countryid,
            co.countryname,
            co.countrycode,
            current_date,
            NULL
        FROM staging.customer cu
        LEFT JOIN staging.cities ci
            ON cu.cityid = ci.cityid
        LEFT JOIN staging.countries co
            ON ci.countryid = co.countryid
        WHERE NOT EXISTS (
            SELECT 1
            FROM dimension.dim_customer dc
            WHERE dc.customer_id = cu.customerid
              AND dc.end_date IS NULL
        );
        """
    )

    # 5. dim_employee
    task_dim_employee = SQLExecuteQueryOperator(
        task_id="dim_employee",
        conn_id="giffari_db",
        sql="""
        INSERT INTO dimension.dim_employee (
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
            end_date
        )
        SELECT
            e.employeeid,
            e.firstname,
            e.middleinitial,
            e.lastname,
            e.birthdate,
            e.gender,
            e.cityid,
            c.cityname,
            co.countryid,
            co.countryname,
            co.countrycode,
            e.hiredate,
            NULL
        FROM staging.employee e
        LEFT JOIN staging.cities c
            ON e.cityid = c.cityid
        LEFT JOIN staging.countries co
            ON c.countryid = co.countryid
        WHERE NOT EXISTS (
            SELECT 1
            FROM dimension.dim_employee de
            WHERE de.employee_id = e.employeeid
        );
        """
    )

    # 6. fact_sales
    task_fact_sales = SQLExecuteQueryOperator(
        task_id="fact_sales",
        conn_id="giffari_db",
        sql="""
        INSERT INTO dimension.fact_sales (
            sk_date,
            sk_customer,
            sk_employee,
            sk_product,
            sales_id,
            transaction_no,
            quantity,
            discount,
            total_price
        )
        SELECT
            dt.sk_date,
            dc.sk_customer,
            de.sk_employee,
            dp.sk_product,
            s.salesid,
            s.transactionnumber,
            s.quantity,
            s.discount,
            (s.quantity * dp.product_price * (1 - s.discount)) AS total_price
        FROM staging.sales s
        LEFT JOIN dimension.dim_time dt
            ON s.salesdate = dt.date
        LEFT JOIN dimension.dim_customer dc
            ON s.customerid = dc.customer_id
           AND dc.end_date IS NULL
        LEFT JOIN dimension.dim_employee de
            ON s.salespersonid = de.employee_id
        LEFT JOIN dimension.dim_product dp
            ON s.productid = dp.product_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM dimension.fact_sales f
            WHERE f.sales_id = s.salesid
        );
        """
    )

    # urutan task
    task_load_staging >> task_dim_time >> task_dim_product >> task_dim_customer >> task_dim_employee >> task_fact_sales