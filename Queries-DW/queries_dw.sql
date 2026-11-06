-- =============================================================================
-- queries_dw.sql
-- OLAP analysis of the retail chain Data Warehouse: Questions 1 to 20 of PDF
-- section 10, in order.
--
-- Course   : DS3003 & DS3004, Data Warehousing and Business Intelligence
-- Student  : Shahoud Shahid, 23i-2515, DS-A
-- Target   : MySQL 8.0. Every query is valid under ONLY_FULL_GROUP_BY.
--
-- How to run
--   The database is chosen by the caller, so this file has no USE. Run it after the
--   program has loaded the warehouse:
--       mysql -h <host> -P <port> -u <user> -p --table <database> < queries_dw.sql
--   The line "-- @query Qn" directly before each statement lets a tool find it.
--
-- DATES ACTUALLY LOADED (PDF section 10 asks for these to be named here)
--   First transaction : 2010-12-01 08:26
--   Last transaction  : 2011-12-09 12:50
--   Calendar 2010 holds December 2010 only (1 to 23 December).
--   Calendar 2011 holds 4 January to 9 December.
--
--   "The loaded year" is 2011, because 2010 holds only one month and so cannot
--   stand for a year. Wherever a question says the year, a query filters on
--   year_no = 2011.
--   "The quarters of the loaded year" are Q1 to Q4 of 2011, and Q4 2011 ends on
--   9 December, so it is about ten weeks long and not thirteen.
--   "The latest six months present" are July to December 2011, and that December
--   ends on the 9th.
--
-- CONVENTIONS USED BY EVERY QUERY
--   Revenue   : always the revenue stored on fact_sales (PDF section 6.2). It is
--               never recomputed from quantity and price.
--   Growth and volatility : the percentage change from the previous period,
--               100 * (current - previous) / previous.
--               LAG runs BEFORE the year is filtered. If the filter ran first, LAG
--               would have nothing to look back at, and January 2011 and Q1 2011
--               would show NULL. Run this way, January 2011 uses December 2010 as its
--               base, and Q1 2011 uses Q4 2010 as its base. NULLIF turns a zero base
--               into NULL and so avoids a division by zero.
--               Every category and every store and supplier pair has sales in all 13
--               loaded months, so the row LAG finds is always the calendar month
--               before.
--   Ties      : a top-N ranking breaks ties by the natural id, so the same data gives
--               the same answer on every run.
--   Keys      : surrogate keys are used only to join and never appear as literals.
--               The loader assigns them in arrival order, so their values mean nothing.
--   Rounding  : money totals stay exact at three decimals, because unit prices have up
--               to three. Averages use three decimals and percentages two, or four
--               for the many small shares in Q13.
--
-- OLAP OPERATIONS (PDF section 11, task 4)
--   Slice      : one dimension held at one value. Time held at 2011 in Q4, Q9, Q10,
--                Q12 and Q18.
--   Dice       : a sub-cube cut on two or more dimensions. Q1 and Q11 (2011, day type,
--                top products) and Q6 (latest six months, city category, marital status).
--   Drill-down : from a coarse level to a finer one. Q1 and Q9 (year to month), Q4 and
--                Q12 (year to quarter), Q13 (store to supplier to product), Q14 (product
--                to season), Q17 (grand total to year, store, supplier and product),
--                Q18 (year to half), Q19 (product to day), Q20 (store to quarter).
--   Materialized view : see the note on Q20.
-- =============================================================================


-- =============================================================================
-- Q1. Top revenue-generating products on weekdays and weekends with monthly
--     drill-down.
-- PDF: Identifies the top 5 products by revenue, split by weekdays and weekends, with
--      monthly breakdowns for the year loaded in the DW.
-- Interpretation: products are ranked ONCE, on their revenue over the whole of 2011,
--      separately for each day type. Those same five products are then broken down by
--      month. A month with no sales for that product and day type has no row.
-- How this differs from Q11: Q1 ranks over the year and then drills down, so each day
--      type has one list of five products. Q11 ranks again inside every month, so the
--      list changes from month to month.
-- OLAP: dice (2011, day type, top products) and drill-down (year to month).
-- =============================================================================
-- @query Q1
WITH year_ranking AS (
    SELECT t.day_type,
           p.product_key,
           SUM(f.revenue) AS year_revenue,
           ROW_NUMBER() OVER (PARTITION BY t.day_type
                              ORDER BY SUM(f.revenue) DESC, p.product_id) AS product_rank
    FROM fact_sales AS f
    JOIN dim_time AS t ON t.time_key = f.time_key
    JOIN dim_product AS p ON p.product_key = f.product_key
    WHERE t.year_no = 2011
    GROUP BY t.day_type, p.product_key, p.product_id
)
SELECT r.day_type,
       r.product_rank,
       p.product_id,
       p.product_name,
       t.month_no,
       t.month_name,
       SUM(f.revenue) AS month_revenue,
       r.year_revenue
FROM year_ranking AS r
JOIN dim_product AS p ON p.product_key = r.product_key
JOIN fact_sales AS f ON f.product_key = r.product_key
JOIN dim_time AS t ON t.time_key = f.time_key AND t.day_type = r.day_type
WHERE r.product_rank <= 5
  AND t.year_no = 2011
GROUP BY r.day_type, r.product_rank, p.product_id, p.product_name,
         t.month_no, t.month_name, r.year_revenue
ORDER BY r.day_type, r.product_rank, t.month_no;


-- =============================================================================
-- Q2. Customer demographics by purchase amount with city category.
-- PDF: Analyses total purchase amount by gender and age, detailed by city category.
-- Interpretation: purchase amount is the sum of revenue over all loaded dates.
-- =============================================================================
-- @query Q2
SELECT c.gender,
       c.age_group,
       c.city_category,
       SUM(f.revenue) AS total_purchase_amount
FROM fact_sales AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
GROUP BY c.gender, c.age_group, c.city_category
ORDER BY c.gender, c.age_group, c.city_category;


-- =============================================================================
-- Q3. Product category sales by occupation.
-- PDF: Examines total sales for each product category based on customer occupation.
-- Interpretation: total revenue over all loaded dates. Inside each category the
--      occupations are listed from highest to lowest sales.
-- =============================================================================
-- @query Q3
SELECT p.product_category,
       c.occupation,
       SUM(f.revenue) AS total_sales
FROM fact_sales AS f
JOIN dim_product AS p ON p.product_key = f.product_key
JOIN dim_customer AS c ON c.customer_key = f.customer_key
GROUP BY p.product_category, c.occupation
ORDER BY p.product_category, total_sales DESC, c.occupation;


-- =============================================================================
-- Q4. Total purchases by gender and age group with quarterly trend.
-- PDF: Tracks purchase amount by gender and age across the quarters of the loaded year.
-- Interpretation: one row per gender and age group, with one column per quarter of
--      2011. The Q4 column ends on 9 December 2011, so it covers less time than the
--      others and a drop there is not by itself a fall in demand.
-- OLAP: slice (year = 2011) and drill-down (year to quarter).
-- =============================================================================
-- @query Q4
SELECT c.gender,
       c.age_group,
       SUM(CASE WHEN t.quarter_no = 1 THEN f.revenue ELSE 0 END) AS q1_2011,
       SUM(CASE WHEN t.quarter_no = 2 THEN f.revenue ELSE 0 END) AS q2_2011,
       SUM(CASE WHEN t.quarter_no = 3 THEN f.revenue ELSE 0 END) AS q3_2011,
       SUM(CASE WHEN t.quarter_no = 4 THEN f.revenue ELSE 0 END) AS q4_2011,
       SUM(f.revenue) AS total_2011
FROM fact_sales AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
JOIN dim_time AS t ON t.time_key = f.time_key
WHERE t.year_no = 2011
GROUP BY c.gender, c.age_group
ORDER BY c.gender, c.age_group;


-- =============================================================================
-- Q5. Top occupations by product category sales.
-- PDF: Highlights the top 5 occupations driving sales within each product category.
-- Interpretation: occupations are ranked by revenue over all loaded dates, separately
--      inside each category, and the first five of each category are kept.
-- =============================================================================
-- @query Q5
WITH ranked AS (
    SELECT p.product_category,
           c.occupation,
           SUM(f.revenue) AS total_sales,
           ROW_NUMBER() OVER (PARTITION BY p.product_category
                              ORDER BY SUM(f.revenue) DESC, c.occupation) AS occupation_rank
    FROM fact_sales AS f
    JOIN dim_product AS p ON p.product_key = f.product_key
    JOIN dim_customer AS c ON c.customer_key = f.customer_key
    GROUP BY p.product_category, c.occupation
)
SELECT product_category,
       occupation_rank,
       occupation,
       total_sales
FROM ranked
WHERE occupation_rank <= 5
ORDER BY product_category, occupation_rank;


-- =============================================================================
-- Q6. City category performance by marital status with monthly breakdown.
-- PDF: Assesses purchase amount by city category and marital status over the latest
--      six months present in the DW.
-- Interpretation: the six months are worked out from the data by DENSE_RANK and are
--      not written into the query. On the loaded file they are July, August, September,
--      October, November and December 2011, and that December ends on the 9th.
-- OLAP: dice (latest six months, city category, marital status).
-- =============================================================================
-- @query Q6
WITH monthly AS (
    SELECT c.city_category,
           c.marital_status,
           t.year_no,
           t.month_no,
           t.month_name,
           SUM(f.revenue) AS purchase_amount,
           DENSE_RANK() OVER (ORDER BY t.year_no DESC, t.month_no DESC) AS month_recency
    FROM fact_sales AS f
    JOIN dim_customer AS c ON c.customer_key = f.customer_key
    JOIN dim_time AS t ON t.time_key = f.time_key
    GROUP BY c.city_category, c.marital_status, t.year_no, t.month_no, t.month_name
)
SELECT city_category,
       marital_status,
       year_no,
       month_no,
       month_name,
       purchase_amount
FROM monthly
WHERE month_recency <= 6
ORDER BY city_category, marital_status, year_no, month_no;


-- =============================================================================
-- Q7. Average purchase amount by stay duration and gender.
-- PDF: Calculates the average purchase amount based on years stayed in the current
--      city and gender.
-- Interpretation: the main answer is AVG(revenue) per transaction, because one fact
--      row is one transaction (PDF section 6.2). A shopping cart can hold many
--      transactions, so a second column gives the average per cart. Over all loaded
--      dates.
-- =============================================================================
-- @query Q7
SELECT c.stay_in_current_city_years,
       c.gender,
       COUNT(*) AS transactions,
       COUNT(DISTINCT f.transaction_id) AS carts,
       ROUND(AVG(f.revenue), 3) AS avg_revenue_per_transaction,
       ROUND(SUM(f.revenue) / COUNT(DISTINCT f.transaction_id), 3) AS avg_revenue_per_cart
FROM fact_sales AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
GROUP BY c.stay_in_current_city_years, c.gender
ORDER BY c.stay_in_current_city_years, c.gender;


-- =============================================================================
-- Q8. Top revenue-generating cities by product category.
-- PDF: Ranks city categories by revenue inside each product category. Only three city
--      categories exist, A, B, and C, so the result lists those categories.
-- Interpretation: RANK inside each category over all loaded dates.
-- =============================================================================
-- @query Q8
SELECT p.product_category,
       RANK() OVER (PARTITION BY p.product_category
                    ORDER BY SUM(f.revenue) DESC) AS city_rank,
       c.city_category,
       SUM(f.revenue) AS revenue
FROM fact_sales AS f
JOIN dim_product AS p ON p.product_key = f.product_key
JOIN dim_customer AS c ON c.customer_key = f.customer_key
GROUP BY p.product_category, c.city_category
ORDER BY p.product_category, city_rank, c.city_category;


-- =============================================================================
-- Q9. Monthly sales growth by product category.
-- PDF: Measures month-over-month sales growth percentage for each product category in
--      the loaded year.
-- Interpretation: LAG runs over all 13 loaded months and the result is then filtered
--      to 2011, so January 2011 uses December 2010 as its base. December 2011 ends on
--      the 9th, so its growth figure compares nine days with a full November.
-- OLAP: slice (year = 2011) and drill-down (year to month).
-- =============================================================================
-- @query Q9
WITH monthly AS (
    SELECT p.product_category,
           t.year_no,
           t.month_no,
           t.month_name,
           SUM(f.revenue) AS revenue
    FROM fact_sales AS f
    JOIN dim_product AS p ON p.product_key = f.product_key
    JOIN dim_time AS t ON t.time_key = f.time_key
    GROUP BY p.product_category, t.year_no, t.month_no, t.month_name
),
with_previous AS (
    SELECT m.*,
           LAG(m.revenue) OVER (PARTITION BY m.product_category
                                ORDER BY m.year_no, m.month_no) AS previous_revenue
    FROM monthly AS m
)
SELECT product_category,
       year_no,
       month_no,
       month_name,
       revenue,
       previous_revenue,
       ROUND(100 * (revenue - previous_revenue) / NULLIF(previous_revenue, 0), 2) AS growth_pct
FROM with_previous
WHERE year_no = 2011
ORDER BY product_category, year_no, month_no;


-- =============================================================================
-- Q10. Weekend vs. weekday sales by age group.
-- PDF: Compares total sales by age group for weekends versus weekdays in the loaded
--      year.
-- Interpretation: conditional sums over 2011, plus the share of weekend sales. The
--      file has no sales on any Saturday, so in this data the weekend means Sunday.
--      The day_type rule itself still treats Saturday and Sunday as the weekend.
-- OLAP: slice (year = 2011).
-- =============================================================================
-- @query Q10
SELECT c.age_group,
       SUM(CASE WHEN t.day_type = 'Weekday' THEN f.revenue ELSE 0 END) AS weekday_revenue,
       SUM(CASE WHEN t.day_type = 'Weekend' THEN f.revenue ELSE 0 END) AS weekend_revenue,
       ROUND(100 * SUM(CASE WHEN t.day_type = 'Weekend' THEN f.revenue ELSE 0 END)
             / NULLIF(SUM(f.revenue), 0), 2) AS weekend_share_pct
FROM fact_sales AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
JOIN dim_time AS t ON t.time_key = f.time_key
WHERE t.year_no = 2011
GROUP BY c.age_group
ORDER BY c.age_group;


-- =============================================================================
-- Q11. Top revenue-generating products on weekdays and weekends, grouped by month.
-- PDF: Finds the top 5 products that generated the highest revenue, separated by
--      weekday and weekend sales, with results grouped by month for the loaded year.
-- Interpretation: products are ranked AGAIN inside every month and day type of 2011,
--      so there are five products for each month and day type pair.
-- How this differs from Q1: Q1 chooses five products from the full year and then shows
--      their months. Q11 chooses five products from each month, so a product can lead
--      in one month and be absent from the next.
-- OLAP: dice (2011, day type, top products in each month).
-- =============================================================================
-- @query Q11
WITH ranked AS (
    SELECT t.month_no,
           t.month_name,
           t.day_type,
           p.product_id,
           p.product_name,
           SUM(f.revenue) AS revenue,
           ROW_NUMBER() OVER (PARTITION BY t.month_no, t.day_type
                              ORDER BY SUM(f.revenue) DESC, p.product_id) AS product_rank
    FROM fact_sales AS f
    JOIN dim_time AS t ON t.time_key = f.time_key
    JOIN dim_product AS p ON p.product_key = f.product_key
    WHERE t.year_no = 2011
    GROUP BY t.month_no, t.month_name, t.day_type, p.product_id, p.product_name
)
SELECT month_no,
       month_name,
       day_type,
       product_rank,
       product_id,
       product_name,
       revenue
FROM ranked
WHERE product_rank <= 5
ORDER BY month_no, day_type, product_rank;


-- =============================================================================
-- Q12. Quarterly store revenue growth.
-- PDF: Calculates the revenue growth rate for each store from one quarter to the next,
--      for the loaded year.
-- Interpretation: LAG runs over all five loaded quarters and the result is then
--      filtered to 2011, so Q1 2011 uses Q4 2010 as its base. Q4 2010 holds only
--      December 2010, which makes the Q1 2011 figure large. Q4 2011 ends on 9 December,
--      so its figure compares a shortened quarter with a full Q3.
-- OLAP: slice (year = 2011) and drill-down (year to quarter).
-- =============================================================================
-- @query Q12
WITH quarterly AS (
    SELECT st.store_name,
           t.year_no,
           t.quarter_no,
           SUM(f.revenue) AS revenue
    FROM fact_sales AS f
    JOIN dim_store AS st ON st.store_key = f.store_key
    JOIN dim_time AS t ON t.time_key = f.time_key
    GROUP BY st.store_name, t.year_no, t.quarter_no
),
with_previous AS (
    SELECT q.*,
           LAG(q.revenue) OVER (PARTITION BY q.store_name
                                ORDER BY q.year_no, q.quarter_no) AS previous_revenue
    FROM quarterly AS q
)
SELECT store_name,
       year_no,
       quarter_no,
       revenue,
       previous_revenue,
       ROUND(100 * (revenue - previous_revenue) / NULLIF(previous_revenue, 0), 2) AS growth_pct
FROM with_previous
WHERE year_no = 2011
ORDER BY store_name, year_no, quarter_no;


-- =============================================================================
-- Q13. Detailed supplier sales contribution by store and product name.
-- PDF: For each store, shows the sales contribution of each supplier, broken down by
--      product name. Group by store, then supplier, then product name.
-- Interpretation: grouped by name and not by id, as the question asks, so two product
--      ids that share one name and one supplier become a single row. Each row shows the
--      revenue of that name, the supplier total inside the store, and both as a share
--      of the store revenue, using window functions. Over all loaded dates.
-- OLAP: drill-down (store to supplier to product).
-- =============================================================================
-- @query Q13
SELECT st.store_name,
       su.supplier_name,
       p.product_name,
       SUM(f.revenue) AS revenue,
       SUM(SUM(f.revenue)) OVER (PARTITION BY st.store_name, su.supplier_name)
           AS supplier_revenue_in_store,
       ROUND(100 * SUM(SUM(f.revenue)) OVER (PARTITION BY st.store_name, su.supplier_name)
             / SUM(SUM(f.revenue)) OVER (PARTITION BY st.store_name), 4)
           AS supplier_share_of_store_pct,
       ROUND(100 * SUM(f.revenue)
             / SUM(SUM(f.revenue)) OVER (PARTITION BY st.store_name), 4)
           AS product_share_of_store_pct
FROM fact_sales AS f
JOIN dim_store AS st ON st.store_key = f.store_key
JOIN dim_supplier AS su ON su.supplier_key = f.supplier_key
JOIN dim_product AS p ON p.product_key = f.product_key
GROUP BY st.store_name, su.supplier_name, p.product_name
ORDER BY st.store_name, su.supplier_name, p.product_name;


-- =============================================================================
-- Q14. Seasonal analysis of product sales.
-- PDF: Presents total sales for each product, drilled down by season: spring, summer,
--      fall, and winter.
-- Interpretation: one row per product with a named column per season, over all loaded
--      dates. Winter therefore adds December 2010, January and February 2011, and
--      1 to 9 December 2011, which is parts of two calendar winters.
-- OLAP: drill-down (product total to season).
-- =============================================================================
-- @query Q14
SELECT p.product_id,
       p.product_name,
       SUM(CASE WHEN t.season = 'Spring' THEN f.revenue ELSE 0 END) AS spring_revenue,
       SUM(CASE WHEN t.season = 'Summer' THEN f.revenue ELSE 0 END) AS summer_revenue,
       SUM(CASE WHEN t.season = 'Fall' THEN f.revenue ELSE 0 END) AS fall_revenue,
       SUM(CASE WHEN t.season = 'Winter' THEN f.revenue ELSE 0 END) AS winter_revenue,
       SUM(f.revenue) AS total_revenue
FROM fact_sales AS f
JOIN dim_product AS p ON p.product_key = f.product_key
JOIN dim_time AS t ON t.time_key = f.time_key
GROUP BY p.product_id, p.product_name
ORDER BY p.product_id;


-- =============================================================================
-- Q15. Store-wise and supplier-wise monthly revenue volatility.
-- PDF: Calculates the month-to-month revenue volatility for each store and supplier
--      pair. Volatility is the percentage change in revenue from one month to the next.
-- Interpretation: all 13 loaded months, December 2010 to December 2011, and no year
--      filter. The first month of each pair has no previous month, so its volatility
--      is NULL. The value keeps its sign, as the PDF defines it as a percentage change.
-- =============================================================================
-- @query Q15
WITH monthly AS (
    SELECT st.store_name,
           su.supplier_name,
           t.year_no,
           t.month_no,
           t.month_name,
           SUM(f.revenue) AS revenue
    FROM fact_sales AS f
    JOIN dim_store AS st ON st.store_key = f.store_key
    JOIN dim_supplier AS su ON su.supplier_key = f.supplier_key
    JOIN dim_time AS t ON t.time_key = f.time_key
    GROUP BY st.store_name, su.supplier_name, t.year_no, t.month_no, t.month_name
),
with_previous AS (
    SELECT m.*,
           LAG(m.revenue) OVER (PARTITION BY m.store_name, m.supplier_name
                                ORDER BY m.year_no, m.month_no) AS previous_revenue
    FROM monthly AS m
)
SELECT store_name,
       supplier_name,
       year_no,
       month_no,
       month_name,
       revenue,
       previous_revenue,
       ROUND(100 * (revenue - previous_revenue) / NULLIF(previous_revenue, 0), 2)
           AS volatility_pct
FROM with_previous
ORDER BY store_name, supplier_name, year_no, month_no;


-- =============================================================================
-- Q16. Top 5 products purchased together.
-- PDF: Identifies the top 5 product pairs that appear together most often in the same
--      shopping cart. A shopping cart is one transaction_id. Count distinct shopping
--      carts, not repeated pairs inside one cart. This can inform product bundling.
-- Interpretation: cart_products removes repeats first, so a product listed twice in one
--      cart still counts once. The self join with a < b then gives each pair once, and
--      never pairs a product with itself. The pair is shown in product id order and ties
--      break on the ids, neither of which depends on how surrogate keys were assigned.
-- =============================================================================
-- @query Q16
WITH product_carts AS (
    SELECT product_key, COUNT(DISTINCT transaction_id) AS carts
    FROM fact_sales
    GROUP BY product_key
),
-- A pair can be in no more carts than either of its two products. So the 5th best pair
-- among the 100 most frequent products is a floor: a pair that could reach the top 5 must
-- have both products in at least that many carts. Only those products are paired below.
-- This gives the exact answer while forming a small fraction of the 9.8 million rows of a
-- full self join, and every join uses the fact index on (transaction_id, product_key).
seed AS (
    SELECT product_key
    FROM product_carts
    ORDER BY carts DESC, product_key
    LIMIT 100
),
seed_pairs AS (
    SELECT COUNT(DISTINCT a.transaction_id) AS carts_together
    FROM seed AS sa
    JOIN fact_sales AS a ON a.product_key = sa.product_key
    JOIN fact_sales AS b
      ON b.transaction_id = a.transaction_id
     AND b.product_key > a.product_key
    JOIN seed AS sb ON sb.product_key = b.product_key
    GROUP BY a.product_key, b.product_key
    ORDER BY carts_together DESC
    LIMIT 5
),
eligible AS (
    -- With fewer than 5 seed pairs (a tiny warehouse) every product stays eligible.
    SELECT pc.product_key
    FROM product_carts AS pc
    WHERE pc.carts >= (SELECT CASE WHEN COUNT(*) = 5 THEN MIN(carts_together) ELSE 1 END
                       FROM seed_pairs)
),
pairs AS (
    -- COUNT(DISTINCT) counts each cart once, even when a product appears in it twice.
    SELECT a.product_key AS key_a,
           b.product_key AS key_b,
           COUNT(DISTINCT a.transaction_id) AS carts_together
    FROM eligible AS ea
    JOIN fact_sales AS a ON a.product_key = ea.product_key
    JOIN fact_sales AS b
      ON b.transaction_id = a.transaction_id
     AND b.product_key > a.product_key
    JOIN eligible AS eb ON eb.product_key = b.product_key
    GROUP BY a.product_key, b.product_key
)
SELECT LEAST(pa.product_id, pb.product_id) AS product_id_1,
       CASE WHEN pa.product_id < pb.product_id THEN pa.product_name
            ELSE pb.product_name END AS product_name_1,
       GREATEST(pa.product_id, pb.product_id) AS product_id_2,
       CASE WHEN pa.product_id < pb.product_id THEN pb.product_name
            ELSE pa.product_name END AS product_name_2,
       pr.carts_together
FROM pairs AS pr
JOIN dim_product AS pa ON pa.product_key = pr.key_a
JOIN dim_product AS pb ON pb.product_key = pr.key_b
ORDER BY pr.carts_together DESC, product_id_1, product_id_2
LIMIT 5;


-- =============================================================================
-- Q17. Yearly revenue by store, supplier, and product with ROLLUP.
-- PDF: Uses ROLLUP to aggregate yearly revenue by store, supplier, and product, so the
--      result shows the product, the supplier subtotal, the store subtotal, and the
--      grand total.
-- Interpretation: over both loaded years. GROUPING() labels each row, because a NULL
--      in a rollup column means "all of them" and the label says so plainly. The extra
--      Year total level comes from having the year in the ROLLUP. The product name is
--      taken with MAX and not grouped on, because a fifth ROLLUP column would add a
--      useless subtotal for every product. A product id has only one name.
-- OLAP: drill-down, read upward as roll-up (grand total, year, store, supplier, product).
-- =============================================================================
-- @query Q17
SELECT CASE
           WHEN GROUPING(t.year_no) = 1 THEN 'Grand total'
           WHEN GROUPING(st.store_name) = 1 THEN 'Year total'
           WHEN GROUPING(su.supplier_name) = 1 THEN 'Store subtotal'
           WHEN GROUPING(p.product_id) = 1 THEN 'Supplier subtotal'
           ELSE 'Product'
       END AS row_level,
       t.year_no,
       st.store_name,
       su.supplier_name,
       p.product_id,
       CASE WHEN GROUPING(p.product_id) = 1 THEN NULL
            ELSE MAX(p.product_name) END AS product_name,
       SUM(f.revenue) AS revenue
FROM fact_sales AS f
JOIN dim_time AS t ON t.time_key = f.time_key
JOIN dim_store AS st ON st.store_key = f.store_key
JOIN dim_supplier AS su ON su.supplier_key = f.supplier_key
JOIN dim_product AS p ON p.product_key = f.product_key
GROUP BY t.year_no, st.store_name, su.supplier_name, p.product_id WITH ROLLUP
ORDER BY GROUPING(t.year_no), t.year_no,
         GROUPING(st.store_name), st.store_name,
         GROUPING(su.supplier_name), su.supplier_name,
         GROUPING(p.product_id), p.product_id;


-- =============================================================================
-- Q18. Revenue and volume by product for the two halves of the year.
-- PDF: For each product, reports quantity sold and revenue for the first half of the
--      year, the second half, and the full year.
-- Interpretation: the year is 2011. H1 is January to June and H2 is July to 9 December,
--      so H2 is the shorter half. A product with no 2011 sales has no row.
-- OLAP: slice (year = 2011) and drill-down (year to half).
-- =============================================================================
-- @query Q18
SELECT p.product_id,
       p.product_name,
       SUM(CASE WHEN t.half_year = 'H1' THEN f.quantity ELSE 0 END) AS h1_quantity,
       SUM(CASE WHEN t.half_year = 'H1' THEN f.revenue ELSE 0 END) AS h1_revenue,
       SUM(CASE WHEN t.half_year = 'H2' THEN f.quantity ELSE 0 END) AS h2_quantity,
       SUM(CASE WHEN t.half_year = 'H2' THEN f.revenue ELSE 0 END) AS h2_revenue,
       SUM(f.quantity) AS year_quantity,
       SUM(f.revenue) AS year_revenue
FROM fact_sales AS f
JOIN dim_product AS p ON p.product_key = f.product_key
JOIN dim_time AS t ON t.time_key = f.time_key
WHERE t.year_no = 2011
GROUP BY p.product_id, p.product_name
ORDER BY p.product_id;


-- =============================================================================
-- Q19. High revenue spikes.
-- PDF: For each product, computes the average daily revenue. Returns the days on which
--      that product's revenue is greater than twice its own daily average.
-- Interpretation: the average is taken over the days the product actually sold, not
--      over every calendar day. A zero-sales day has no fact row, and counting those days
--      would lower the average and report far more spikes. Over all loaded dates.
--      The test multiplies instead of dividing by an average, because 34 product-days sit
--      at exactly twice their average, and a rounded average could misjudge those.
-- OLAP: drill-down (product to day).
-- =============================================================================
-- @query Q19
WITH daily AS (
    SELECT f.product_key,
           t.full_date,
           SUM(f.revenue) AS daily_revenue
    FROM fact_sales AS f
    JOIN dim_time AS t ON t.time_key = f.time_key
    GROUP BY f.product_key, t.full_date
),
scored AS (
    SELECT d.product_key,
           d.full_date,
           d.daily_revenue,
           SUM(d.daily_revenue) OVER (PARTITION BY d.product_key) AS total_revenue,
           COUNT(*) OVER (PARTITION BY d.product_key) AS days_sold
    FROM daily AS d
)
SELECT p.product_id,
       p.product_name,
       s.full_date,
       s.daily_revenue,
       ROUND(s.total_revenue / s.days_sold, 3) AS avg_daily_revenue,
       s.days_sold,
       ROUND(s.daily_revenue * s.days_sold / s.total_revenue, 2) AS times_average
FROM scored AS s
JOIN dim_product AS p ON p.product_key = s.product_key
WHERE s.daily_revenue * s.days_sold > 2 * s.total_revenue
ORDER BY p.product_id, s.full_date;


-- =============================================================================
-- Q20. View STORE_QUARTERLY_SALES.
-- PDF: Creates a view named STORE_QUARTERLY_SALES. It returns total quarterly sales by
--      store. When you read the view, order the tuples by store name and then by
--      quarter. MySQL does not keep ORDER BY inside CREATE VIEW, so put the ordering on
--      the SELECT that reads the view.
-- Why a plain view: PDF section 11 task 4 mentions a materialized view, but MySQL has no
--      materialized views, and PDF section 6.2 forbids storing business totals as a
--      second fact table. A plain view keeps the definition in one place and always
--      agrees with the fact table. Nothing is stored, so it cannot go stale.
-- Interpretation: the five loaded quarters are Q4 2010 (December only) and Q1 to Q4 2011
--      (Q4 ending on 9 December). The reading query also orders by year, because
--      quarter 4 appears in both years.
-- OLAP: drill-down (store to quarter).
-- =============================================================================
-- @query Q20_view
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT st.store_name,
       t.year_no,
       t.quarter_no,
       SUM(f.revenue) AS total_sales
FROM fact_sales AS f
JOIN dim_store AS st ON st.store_key = f.store_key
JOIN dim_time AS t ON t.time_key = f.time_key
GROUP BY st.store_name, t.year_no, t.quarter_no;

-- @query Q20
SELECT store_name,
       year_no,
       quarter_no,
       total_sales
FROM STORE_QUARTERLY_SALES
ORDER BY store_name, year_no, quarter_no;
