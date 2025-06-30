-- total revenues by month 
SELECT EXTRACT(YEAR FROM order_date) AS Year, 
		EXTRACT(MONTH FROM order_date) AS Month,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT customer_key) AS total_customers
FROM sales_view
GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date);

--running sum of total_revenues by month
WITH monthly_total_revenues AS (
SELECT DATE_TRUNC('month', order_date) AS order_date,
	SUM(sales_amount) AS total_sales
FROM sales_view
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date)
)

SELECT 
	order_date,
	SUM(total_sales) OVER(ORDER BY order_date ) AS running_total_sales
FROM monthly_total_revenues

-- performance analysis of product sales
WITH product_total_revenues AS 
(
SELECT EXTRACT(Year FROM order_date) AS Year,
		products.product_name,
		SUM(sales.sales_amount) AS total_revenues
FROM products_view AS products
JOIN sales_view AS sales ON sales.product_key = products.product_key
GROUP BY EXTRACT(Year FROM order_date), products.product_name
ORDER BY 1,2
)

SELECT year, 
	   product_name,
	   total_revenues,
	   AVG(total_revenues)OVER(PARTITION BY product_name) AS avg_sales,
	   CASE
		   WHEN total_revenues >= AVG(total_revenues)OVER(PARTITION BY product_name) THEN 'Above Average'
		   ELSE 'Below Average'
	   END AS avg_change,
	   CASE 
	   	WHEN LAG(total_revenues)OVER(PARTITION BY product_name ORDER BY year) IS NULL THEN 'No Change'
	   	WHEN total_revenues < LAG(total_revenues)OVER(PARTITION BY product_name ORDER BY year) THEN 'Decrease'
		ELSE 'Increase'
	   END AS py_change
FROM product_total_revenues

/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/
WITH sales_cte AS (
SELECT sales.customer_key, 
		SUM(sales_amount) AS spending,
		1+ EXTRACT (MONTH FROM AGE(MAX(DATE_TRUNC('Month', order_date)), MIN(DATE_TRUNC('Month', order_date)) )) AS month_range
FROM sales_view AS sales
GROUP BY sales.customer_key
ORDER BY 1
),
segment_cte AS 
(SELECT 
	customer_key, spending, month_range,
	CASE
		WHEN month_range >=12 AND spending >=5000 THEN 'VIP'
		WHEN month_range >=12 AND spending <5000 THEN 'Regular'
		ELSE 'New'
	END AS segment
FROM sales_cte
)

SELECT 
	segment, COUNT(segment)
FROM segment_cte
GROUP BY segment

/*products segmentation
*/
SELECT
CASE
	WHEN cost < 100 THEN 'Below 100'
	WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	ELSE '500-1000'
END AS product_segment,
COUNT(product_key)
FROM products_view
GROUP BY 
CASE
	WHEN cost < 100 THEN 'Below 100'
	WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	ELSE '500-1000'
END 

--for tableua
SELECT 
	ROW_NUMBER()OVER(ORDER BY order_number),
	sales.order_number,
	sales.order_date, 
	sales.shipping_date, 
	sales.customer_key, 
	customers.first_name ||' '|| customers.last_name AS customer_name,
	customers.country,
	customers.marital_status,
	customers.gender,
	products.product_key, 
	products.category,
	products.subcategory,
	products.product_name,
	products.cost AS product_cost,
	sales.sales_amount,
	sales.quantity,
	sales_amount - products.cost*sales.quantity AS profit

FROM sales_view AS sales 
JOIN products_view AS products ON sales.product_key = products.product_key
JOIN customers_view AS customers ON sales.customer_key = customers.customer_key


