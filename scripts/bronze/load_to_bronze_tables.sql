
/*
We are using COPY like BULK INSERT IN SQL to fill our tables

It is very nice and fast way to extract data from source and insert to tables
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
        TRUNCATE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
            FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_crm/cust_info.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');

        TRUNCATE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
            FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_crm/prd_info.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');

        TRUNCATE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
            FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_crm/sales_details.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');

        TRUNCATE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
            FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_erp/cust_az12.csv'
            WITH (format csv, header true, delimiter ',');

        TRUNCATE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
            FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_erp/loc_a101.csv'
            WITH (format csv, header true, delimiter ',');

        TRUNCATE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
            FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_erp/px_cat_g1v2.csv'
            WITH (format csv, header true, delimiter ',');
END;
$$;

call bronze.load_bronze();

select count(*) from bronze.crm_cust_info;
select count(*) from bronze.crm_prd_info;
select count(*) from bronze.crm_sales_details;
select count(*) from bronze.erp_cust_az12;
select count(*) from bronze.erp_loc_a101;
select count(*) from bronze.erp_px_cat_g1v2;