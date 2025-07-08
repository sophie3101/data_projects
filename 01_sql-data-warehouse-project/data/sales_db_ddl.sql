DROP TABLE IF EXISTS customer_order_info;
DROP TABLE IF EXISTS product_info;
DROP TABLE IF EXISTS sales_details;
DROP TABLE IF EXISTS customer_location;
DROP TABLE IF EXISTS customer_info;
DROP TABLE IF EXISTS product_categories;

CREATE TABLE product_categories (
    id           VARCHAR(50) PRIMARY KEY,
    cat          VARCHAR(50) NOT NULL,
    subcat       VARCHAR(50) NOT NULL,
    maintenance   BOOLEAN DEFAULT FALSE
);

CREATE TABLE customer_info (
    cid    VARCHAR(50) PRIMARY KEY,
    bdate  DATE,
    gen    VARCHAR(50)
);

CREATE TABLE customer_location (
    cid    VARCHAR(50) PRIMARY KEY,
    cntry  VARCHAR(50) NOT NULL,
    FOREIGN KEY(cid) REFERENCES customer_info(cid)
);

CREATE TABLE customer_order_info (
  cst_id	     				  INT PRIMARY KEY,
  cst_key      				  VARCHAR(50) UNIQUE NOT NULL,
  cst_firstname   			VARCHAR(50) NOT NULL,
  cst_lastname					VARCHAR(50) NOT NULL,
  cst_marital_status    VARCHAR(50) NOT NULL,
  cst_gndr 					    VARCHAR(50) NOT NULL,
  cst_create_date				Date,
  FOREIGN KEY (cst_key) REFERENCES customer_info(cid)
);

CREATE TABLE product_info(
  prd_id						    INT PRIMARY KEY,
  prd_key					      VARCHAR(50) NOT NULL,
  prd_nm						    VARCHAR(50) NOT NULL,
  prd_cost					    FLOAT,
  prd_line					    VARCHAR(50),
  prd_start_dt				  DATE,
  prd_end_dt					  DATE,
  prd_cat_id            VARCHAR(50) NOT NULL,
  prd_sub_key           VARCHAR(50) NOT NULL,
  FOREIGN KEY(prd_cat_id) REFERENCES product_categories(id)
);

CREATE TABLE sales_details(
  sls_ord_num  VARCHAR(50) ,
  sls_prd_key  VARCHAR(50),
  sls_cust_id  INT,
  sls_order_dt DATE NULL,
  sls_ship_dt  DATE,
  sls_due_dt   DATE,
  sls_sales    FLOAT,
  sls_quantity INT,
  sls_price    FLOAT,
  UNIQUE(sls_ord_num, sls_prd_key)
);


