
/*
We are using COPY like BULK INSERT IN SQL to fill our tables

It is very nice and fast way to extract data from source and insert to tables
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
    DECLARE start_time TIMESTAMP;
            end_time TIMESTAMP;
            batch_start_time TIMESTAMP;
            batch_end_time TIMESTAMP;
BEGIN
        batch_start_time := clock_timestamp();
    RAISE NOTICE '==================================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '==================================================================';

    RAISE NOTICE '------------------------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------------------------';
        start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
        FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        end_time := clock_timestamp();
        RAISE NOTICE '>> Time Taken: %ms', (extract(epoch from end_time - start_time) * 1000)::INT;

        start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE bronze.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
        FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        end_time := clock_timestamp();
        RAISE NOTICE '>> Time Taken: %ms', (extract(epoch from end_time - start_time) * 1000)::INT;

        start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE bronze.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
        FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
        end_time := clock_timestamp();
        RAISE NOTICE '>> Time Taken: %ms', (extract(epoch from end_time - start_time) * 1000)::INT;

    RAISE NOTICE '------------------------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------------------------';

        start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE bronze.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
        FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_erp/cust_az12.csv'
        WITH (format csv, header true, delimiter ',');
        end_time := clock_timestamp();
        RAISE NOTICE '>> Time Taken: %ms', (extract(epoch from end_time - start_time) * 1000)::INT;
        start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE bronze.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
        FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_erp/loc_a101.csv'
        WITH (format csv, header true, delimiter ',');
        end_time := clock_timestamp();
        RAISE NOTICE '>> Time Taken: %ms', (extract(epoch from end_time - start_time) * 1000)::INT;

        start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
        FROM 'D:/DataEngineering/sqldatawarehouse/datasets/source_erp/px_cat_g1v2.csv'
        WITH (format csv, header true, delimiter ',');
        end_time := clock_timestamp();
        RAISE NOTICE '>> Time Taken: %ms', (extract(epoch from end_time - start_time) * 1000)::INT;
    batch_end_time := clock_timestamp();

        RAISE NOTICE '==============================================================';
        RAISE NOTICE 'Loading Bronze Layer is Completed';
        RAISE NOTICE '- Total Load Duration: % ms', (extract(epoch from batch_end_time - batch_start_time) * 1000)::INT;
        RAISE NOTICE '==============================================================';

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '==================================================================';
        RAISE NOTICE 'Loading Bronze Layer Failed';
        RAISE NOTICE 'Error: %', SQLERRM;
        RAISE NOTICE 'Error: %', SQLSTATE;
        RAISE NOTICE '==================================================================';
END;
$$;

call bronze.load_bronze();
