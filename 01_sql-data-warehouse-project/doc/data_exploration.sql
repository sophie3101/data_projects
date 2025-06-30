-- what countries customers are from
SELECT DISTINCT country
FROM customers_view;

-- "United Kingdom"
-- "Australia"
-- "Germany"
-- "Canada"
-- "N/A"
-- "France"
-- "United States"

-- What product cateogry in the products
SELECT DISTINCT category
FROM products_view;

-- "Bikes"
-- "Accessories"
-- "Clothing"
-- "Components"

-- What product cateogry, subcategory,product in the products
SELECT DISTINCT category, subcategory, product_name
FROM products_view
ORDER BY 1,2

-- first  and last order date
SELECT MIN(order_date) as first_date, MAX(order_date) as last_date
FROM sales_view;

-- range of years of purchases
SELECT EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date)) as year_range
FROM sales_view;

--total sales
SELECT SUM(sales_amount) AS total_sales
FROM sales_view;

--how many items are sold
SELECT SUM(quantity) AS total_quantity
FROM sales_view;

--average selling price
SELECT AVG(price) AS avg_selling_price
FROM sales_view;

-- total number of orders
SELECT COUNT(DISTINCT order_number) AS total_order_num
FROM sales_view;

--total number of products
SELECT COUNT(product_key)
FROM products_view;


-- --total number of customers
SELECT COUNT(customer_key)
FROM customers_view;

SELECT COUNT(distinct customer_key) 
FROM sales_view;

--Number of customer by countries
SELECT country, COUNT(customer_key) AS customer_count
FROM customers_view
GROUP BY country;

-- Number of customer by gender
SELECT gender, COUNT(customer_key) AS customer_count
FROM customers_view
GROUP BY gender;

--total products by category
SELECT category, COUNT(product_key)  AS product_count
FROM products_view
GROUP BY category;

-- average cost in each category
SELECT category, AVG(cost) AS avg_cost
FROM products_view
GROUP BY category;

--total revenue in each category
SELECT products.category,
SUM(sales.sales_amount) AS total_revenue
FROM sales_view AS sales
JOIN products_view AS products ON products.product_key = sales.product_key
GROUP BY products.category;

--total revenue by each customer
SELECT customers.customer_key,
	customers.first_name,
	customers.last_name,
	SUM(sales.sales_amount) AS total_revenue
FROM sales_view AS sales
JOIN customers_view AS customers ON customers.customer_key = sales.customer_key
GROUP BY customers.customer_key,customers.first_name,customers.last_name
ORDER BY total_revenue DESC

--which 5 product generate highest revenue
SELECT products.product_name,
SUM(sales.sales_amount) AS total_revenue
FROM sales_view AS sales
JOIN products_view AS products ON products.product_key = sales.product_key
GROUP BY products.product_name
ORDER BY total_revenue DESC
LIMIT 5

--which 5 product generate lowest revenue
SELECT products.product_name,
SUM(sales.sales_amount) AS total_revenue
FROM sales_view AS sales
JOIN products_view AS products ON products.product_key = sales.product_key
GROUP BY products.product_name
ORDER BY total_revenue 
LIMIT 5