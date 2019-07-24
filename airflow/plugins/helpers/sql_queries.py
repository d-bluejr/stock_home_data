class SqlQueries:
    #SCHEMA
    schema = "stock_home_data"

    #TABLES
    
    # The table names are stored in variables to make modifications of table names in queries simplier and unified
    staging_sales_count = "staging_sales_count"
    staging_sales_price = "staging_sales_price"
    staging_listing_price = "staging_listing_price"
    staging_listing_price_sqft = "staging_listing_price_sqft"
    staging_home_values = "staging_home_values"
    staging_home_values_sqft = "staging_home_values_sqft"
    staging_stocks = "staging_stocks"
    staging_etfs = "staging_etfs"
    staging_companies = "staging_companies"

    home_value = "home_values"
    home_value_sqft = "home_values_per_sqft"
    home_listing = "home_listing_prices"
    home_listing_sqft = "home_listing_prices_per_sqft"
    home_sales_price = "home_sales_prices"
    home_sales_count = "home_sales_count"
    stock_open = "stock_open_prices"
    stock_high = "stock_highs"
    stock_low = "stock_lows"
    stock_close = "stock_close_prices"
    stock_volume = "stock_volumes"
    etf_open = "etf_open_prices"
    etf_high = "etf_highs"
    etf_low = "etf_lows"
    etf_close = "etf_closing_prices"
    etf_volume = "etf_volumes"

    companies = "companies"
    state = "states"
    date = "dates"

    staging_table_list = [staging_sales_count, staging_sales_price, staging_listing_price, staging_listing_price_sqft,
                          staging_home_values, staging_home_values_sqft, staging_stocks, staging_etfs,
                          staging_companies]

    dim_table_list = [companies, state, date]

    fact_table_list = [home_value, home_value_sqft, home_listing, home_listing_sqft, home_sales_price,
                       home_sales_count, stock_open, stock_close, stock_high, stock_low, stock_volume, etf_open,
                       etf_high, etf_low, etf_close, etf_volume]

    # DROP TABLES

    # The beginning part of all the drop queries are the same, so a variable is used to store and reuse that
    drop_starter = "DROP TABLE IF EXISTS public.{}"

    # CREATE TABLES
    # The create queries are created using the create starter variable, the table name, and the list of column definitions

    # The beginning part of all the create queries are the same, so a variable is used to store and reuse that
    create_starter = "CREATE TABLE IF NOT EXISTS public.{}"

    staging_count_value_create = ("""
            (
	            "state" varchar(256),
	            "date" varchar(256),
                "value" numeric(7,0)
            );
        """)

    staging_price_create = ("""
            (
    	        "state" varchar(256),
    	        "date" varchar(256),
    	        "value" numeric(10,2)
            );
        """)

    staging_sqft_create = ("""
            (
    	        "state" varchar(256),
    	        "date" varchar(256),
    	        "value" numeric(8,4)
            );
        """)

    staging_stock_etf_create = (""" 
        (
    	    Company_Abbr varchar(16),
    	    "Date" varchar(256),
    	    "Open" numeric(15,5),
            "High" numeric(15,5),
    	    "Low" numeric(15,5),
    	    "Close" numeric(15,5),
    	    "Volume" numeric(11,0),
    	    "OpenInt" numeric(1,0)
        );
    """)

    staging_company_create = ("""
        (
            ticker varchar(256), 
            company_name varchar(256), 
            short_name varchar(256), 
            industry varchar(256), 
            description varchar(4096), 
            website varchar(256), 
            logo varchar(256), 
            ceo varchar(256), 
            exchange varchar(256), 
            market_cap varchar(256), 
            sector varchar(256), 
            tag_1 varchar(256), 
            tag_2 varchar(256), 
            tag_3 varchar(256)
        );
    """)

    home_value_create = (""" 
            (
                state_id int NOT NULL,
        	    date_id int NOT NULL,
        	    home_value numeric(7,0),
        	    CONSTRAINT hv_pkey PRIMARY KEY (state_id, date_id)
            );
        """)

    home_value_sqft_create = (""" 
            (
                state_id int NOT NULL,
                date_id int NOT NULL,
                value_per_sqft numeric(8,4),
        	    CONSTRAINT hvs_pkey PRIMARY KEY (state_id, date_id)
            );
        """)

    home_listing_create = (""" 
            (
                state_id int NOT NULL,
                date_id int NOT NULL,
                list_price numeric(10,2),
        	    CONSTRAINT hlp_pkey PRIMARY KEY (state_id, date_id)
            );
        """)

    home_listing_sqft_create = (""" 
            (
                state_id int NOT NULL,
                date_id int NOT NULL,
                list_price_per_sqft numeric(10,4),
        	    CONSTRAINT hls_pkey PRIMARY KEY (state_id, date_id)
            );
        """)

    home_sales_price_create = (""" 
            (
                state_id int NOT NULL,
                date_id int NOT NULL,
                sales_price numeric(10,2),
        	    CONSTRAINT hsp_pkey PRIMARY KEY (state_id, date_id)
            );
        """)

    home_sales_count_create = (""" 
            (
                state_id int NOT NULL,
                date_id int NOT NULL,
                sales_count numeric(7,0),
        	    CONSTRAINT hsc_pkey PRIMARY KEY (state_id, date_id)
            );
        """)

    stock_open_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                open_price numeric(15,5),
        	    CONSTRAINT so_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    stock_highs_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                high_value numeric(15,5),
        	    CONSTRAINT sh_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    stock_lows_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                low_value numeric(15,5),
        	    CONSTRAINT sl_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    stock_close_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                close_price numeric(15,5),
        	    CONSTRAINT sc_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    stock_volume_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                volume numeric(11,0),
        	    CONSTRAINT sv_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)
        
    etf_open_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                open_price numeric(15,5),
        	    CONSTRAINT eo_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    etf_highs_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                high_value numeric(15,5),
        	    CONSTRAINT eh_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    etf_lows_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                low_value numeric(15,5),
        	    CONSTRAINT el_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    etf_close_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                close_price numeric(15,5),
        	    CONSTRAINT ec_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    etf_volume_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                date_id int NOT NULL,
                volume numeric(11,0),
        	    CONSTRAINT ev_pkey PRIMARY KEY (company_abbr, date_id)
            );
        """)

    company_create = (""" 
            (
                company_abbr varchar(16) NOT NULL,
                full_name varchar(256),
	            short_name varchar(256),
	            industry varchar(256),
	            sector varchar(256),
	            tag_1 varchar(256),
	            tag_2 varchar(256),
	            tag_3 varchar(256),
	            CONSTRAINT company_pkey PRIMARY KEY (company_abbr)
            );
        """)

    state_create = (""" 
            (
    	        state_id int IDENTITY(1,1),
    	        state_name varchar(256) NOT NULL,
    	        CONSTRAINT state_pkey PRIMARY KEY (state_id)
            );
        """)

    date_create = (""" 
            (
	            date_id int IDENTITY(1,1),
	            date_chars varchar(256),
	            "year" int4,
	            "month" int4,
	            "day" int4,
	            CONSTRAINT date_pkey PRIMARY KEY (date_id)
            );
        """)

    # STAGING TABLE LOAD

    #The copy commands used to stage the song and event files into the staging tables.
    staging_json_copy = ("""
        copy {} from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}' 
        JSON 'auto'
        compupdate off region 'us-west-2';
    """)
    
    staging_csv_copy = ("""
        copy {} from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        DELIMITER ','
        EMPTYASNULL
        BLANKSASNULL
        IGNOREBLANKLINES
        FILLRECORD
        ESCAPE
        REMOVEQUOTES
        compupdate off region 'us-west-2';
    """)

    # INSERT TABLES

    # The beginning and middle part of all the insert queries are the same, so variables are used to store and reuse those
    insert_starter = "INSERT INTO {}"

    value_insert = ("""
        SELECT states.state_id, dates.date_id, values.value as home_value
        FROM staging_home_values values
        LEFT JOIN dates
        ON values.date = dates.date_chars
        LEFT JOIN states
        ON values.state = states.state_name
    """)

    value_sqft_insert = ("""
        SELECT states.state_id, dates.date_id, values.value as value_per_sqft
        FROM staging_home_values_sqft values
        LEFT JOIN dates
        ON values.date = dates.date_chars
        LEFT JOIN states
        ON values.state = states.state_name
    """)

    listing_price_insert = ("""
        SELECT states.state_id, dates.date_id, price.value as listing_price
        FROM staging_listing_price price
        LEFT JOIN dates
        ON price.date = dates.date_chars
        LEFT JOIN states
        ON price.state = states.state_name
    """)

    listing_price_sqft_insert = ("""
        SELECT states.state_id, dates.date_id, price.value as listing_price_per_sqft
        FROM staging_listing_price_sqft price
        LEFT JOIN dates
        ON price.date = dates.date_chars
        LEFT JOIN states
        ON price.state = states.state_name
    """)

    sales_price_insert = ("""
        SELECT states.state_id, dates.date_id, price.value as sales_price
        FROM staging_sales_price price
        LEFT JOIN dates
        ON price.date = dates.date_chars
        LEFT JOIN states
        ON price.state = states.state_name
    """)

    sales_count_insert = ("""
        SELECT states.state_id, dates.date_id, count.value as sales_count
        FROM staging_sales_count count
        LEFT JOIN dates
        ON count.date = dates.date_chars
        LEFT JOIN states
        ON count.state = states.state_name
    """)

    stock_open_insert = ("""
        SELECT stocks.company_abbr, dates.date_id, stocks."Open" as open_price
        FROM staging_stocks stocks
        LEFT JOIN dates
        ON stocks.date = dates.date_chars
    """)

    stock_high_insert = ("""
        SELECT stocks.company_abbr, dates.date_id, stocks."High" as high_value
        FROM staging_stocks stocks
        LEFT JOIN dates
        ON stocks.date = dates.date_chars
    """)

    stock_low_insert = ("""
        SELECT stocks.company_abbr, dates.date_id, stocks."Low" as low_value
        FROM staging_stocks stocks
        LEFT JOIN dates
        ON stocks.date = dates.date_chars
    """)

    stock_close_insert = ("""
        SELECT stocks.company_abbr, dates.date_id, stocks."Close" as close_price
        FROM staging_stocks stocks
        LEFT JOIN dates
        ON stocks.date = dates.date_chars
    """)

    stock_volume_insert = ("""
        SELECT stocks.company_abbr, dates.date_id, stocks."Volume" as volume
        FROM staging_stocks stocks
        LEFT JOIN dates
        ON stocks.date = dates.date_chars
    """)

    etf_open_insert = ("""
        SELECT etf.company_abbr, dates.date_id, etf."Open" as open_price
        FROM staging_etfs etf
        LEFT JOIN dates
        ON etf.date = dates.date_chars
    """)

    etf_high_insert = ("""
        SELECT etf.company_abbr, dates.date_id, etf."High" as high_value
        FROM staging_etfs etf
        LEFT JOIN dates
        ON etf.date = dates.date_chars
    """)

    etf_low_insert = ("""
        SELECT etf.company_abbr, dates.date_id, etf."Low" as low_value
        FROM staging_etfs etf
        LEFT JOIN dates
        ON etf.date = dates.date_chars
    """)
    
    etf_close_insert = ("""
        SELECT etf.company_abbr, dates.date_id, etf."Close" as close_price
        FROM staging_etfs etf
        LEFT JOIN dates
        ON etf.date = dates.date_chars
    """)

    etf_volume_insert = ("""
        SELECT etf.company_abbr, dates.date_id, etf."Volume" as volume
        FROM staging_etfs etf
        LEFT JOIN dates
        ON etf.date = dates.date_chars
    """)

    company_insert = ("""
        SELECT ticker as company_abbr, company_name as full_name, short_name, industry, sector, tag_1, tag_2, tag_3
        FROM staging_companies
    """)

    state_insert = (""" (state_name)
        SELECT distinct "state" as state_name
        FROM staging_sales_count
    """)

    date_insert = (""" (date_chars, year, month, day)
        SELECT DISTINCT date as date_chars, SPLIT_PART(date,'-',1)::integer as year, SPLIT_PART(date,'-',2)::integer as month, 0 as day
        FROM (
            SELECT date 
            FROM staging_sales_count
            UNION
            SELECT date 
            FROM staging_sales_price
            UNION
            SELECT date 
            FROM staging_listing_price
            UNION
            SELECT date 
            FROM staging_listing_price_sqft
            UNION
            SELECT date 
            FROM staging_home_values
            UNION
            SELECT date 
            FROM staging_home_values_sqft
        )
        UNION
        SELECT DISTINCT date as date_chars, SPLIT_PART(date,'-',1)::integer as year, SPLIT_PART(date,'-',2)::integer as month, 
        SPLIT_PART(date,'-',3)::integer as day
        FROM (
            SELECT Date as date 
            FROM staging_stocks
            UNION
            SELECT Date as date
            FROM staging_etfs
        )
    """)
    