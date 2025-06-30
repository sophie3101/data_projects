DROP VIEW IF EXISTS customers_view;
CREATE VIEW customers_view AS 
SELECT 
ROW_NUMBER() OVER(ORDER BY c_order.cst_id ) AS customer_key,
c_order.cst_id AS customer_id,
c_order.cst_key AS customer_number,
c_order.cst_firstname AS first_name,
c_order.cst_lastname AS last_name,
c_loc.cntry AS country,
c_order.cst_marital_status AS marital_status,
CASE
	WHEN c_order.cst_gndr<>'N/A' THEN c_order.cst_gndr
	ELSE COALESCE(c_info.gen, 'N/A')
END AS gender,
c_info.bdate AS birth_date,
c_order.cst_create_date AS create_date
FROM customer_order_info AS c_order
JOIN customer_location AS c_loc ON c_loc.CID = c_order.cst_key
JOIN customer_info AS c_info ON c_info.CID = c_order.cst_key;


DROP VIEW  IF EXISTS products_view;
CREATE VIEW products_view AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_key, prd_start_dt) AS product_key,
	product.prd_id AS product_id,
	product.prd_sub_key AS product_number,
	product.prd_nm AS product_name,
	product.prd_cat_id AS category_id,
	cat.cat AS category,
	cat.subcat AS subcategory,
	cat.maintenance AS maintenance,
	product.prd_cost AS cost,
	product.prd_line AS product_line,
	product.prd_start_dt AS start_date
FROM product_info AS product
JOIN product_categories AS cat ON cat.ID = product.prd_cat_id;


DROP VIEW IF EXISTS sales_view;
CREATE VIEW sales_view AS 
SELECT 
	sales.sls_ord_num  AS order_number,
    products.product_key  AS product_key,
    customers.customer_key AS customer_key,
    sales.sls_order_dt AS order_date,
    sales.sls_ship_dt  AS shipping_date,
    sales.sls_due_dt   AS due_date,
    sales.sls_sales    AS sales_amount,
    sales.sls_quantity AS quantity,
    sales.sls_price    AS price
FROM sales_details AS sales
JOIN products_view AS products ON sales.sls_prd_key = products.product_number
JOIN customers_view AS customers ON customers.customer_id = sales.sls_cust_id;


CREATE VIEW orders_view AS
SELECT 
ROW_NUMBER()OVER(ORDER BY order_number) AS id,
* 
FROM
(SELECT 
	sales.sls_ord_num  AS order_number,
	sales.sls_order_dt AS order_date,
	sales.sls_ship_dt  AS shipping_date,
	customers.customer_key AS customer_key,
    customers.first_name ||' '|| customers.last_name AS customer_name,
	customers.country,
	customers.marital_status,
	customers.gender,
	products.product_key  AS product_key,
	products.category,
	products.subcategory,
	products.product_name,
	products.cost AS product_cost,
    sales.sls_sales    AS sales_amount,
    sales.sls_quantity AS quantity,
    sales.sls_price    AS price,
	RANK()OVER(PARTITION BY sales.sls_ord_num,products.product_number ORDER BY products.cost)
FROM sales_details AS sales
JOIN products_view AS products ON sales.sls_prd_key = products.product_number
JOIN customers_view AS customers ON customers.customer_id = sales.sls_cust_id
)
WHERE rank=1


