TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)

SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) as cst_firstname,
    TRIM(cst_lastname) as cst_lastname,
    CASE WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
         WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a' END as cst_marital_status,
    CASE WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
         WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
         ELSE 'n/a' END as cst_gndr,
    cst_create_date
FROM (
         SELECT
             *,
             ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as rn
         from bronze.crm_cust_info
         WHERE cst_id is not null
     ) ranked
WHERE rn = 1;

select count(*) from silver.crm_cust_info;
select count(*) from bronze.crm_cust_info;
select count(*) from bronze.crm_cust_info where cst_id is not null;

select cst_id, count(*) from bronze.crm_cust_info group by cst_id having count(*) > 1;

select count(*) from silver.crm_cust_info group by cst_id having count(*) > 1 or cst_id is null;
select count(*) from silver.crm_cust_info where trim(cst_firstname) != cst_firstname;
select count(*) from silver.crm_cust_info where trim(cst_lastname) != cst_lastname;
select distinct cst_marital_status from silver.crm_cust_info;
select distinct cst_gndr from silver.crm_cust_info;