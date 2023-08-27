-- DROP TABLE IF EXISTS report.agg_daily;
-- CREATE TABLE report.agg_daily(

-- 	working_day date,
-- 	running_total numeric,
-- 	total_paid numeric,
-- 	previous_day numeric,
-- 	previous_daily_difference numeric,
-- 	avg_customer_paid numeric,
-- 	avg_days_rental_duration numeric
-- );

-----To insert values from 1 table schema to the other
-- SELECT * INTO public.TEMP_DAILY_AGGREGATE_REVENUE FROM public.TEMP_DAILY_AGGREGATE_REVENUE;
-- SELECT * FROM public.TEMP_DAILY_AGGREGATE_REVENUE

--------------------------------------------------------
--Droping and creating the report schema

DROP SCHEMA IF EXISTS report CASCADE;
CREATE SCHEMA report;

-- Tables for all payments
--1-- 
DROP INDEX IF EXISTS idx_rental_id_temp_table;
DROP TABLE IF EXISTS TEMP_ALL_PAYMENTS_TABLE;
CREATE TABLE TEMP_ALL_PAYMENTS_TABLE AS (
    SELECT
		rentals.customer_id,
        rentals.rental_id,
		rentals.inventory_id,
		se_inventory.film_id,
		se_film.rating,
        payment.amount,
		DATE(rentals.rental_date) AS rental_date,
        DATE(rentals.return_date) AS return_date
	FROM public.rental AS rentals
    INNER JOIN public.payment AS payment
    	ON rentals.rental_id = payment.rental_id
	INNER JOIN public.inventory AS se_inventory
    	ON se_inventory.inventory_id = rentals.inventory_id
	INNER JOIN public.film AS se_film
		ON se_film.film_id = se_inventory.film_id
);
CREATE INDEX idx_rental_id_temp_table ON TEMP_ALL_PAYMENTS_TABLE(rental_id);
SELECT * FROM TEMP_ALL_PAYMENTS_TABLE;

--2--
DROP INDEX IF EXISTS idx_rental_id_temp_table_daily;
DROP TABLE IF EXISTS DAILY_AGGREGATE;
CREATE TABLE DAILY_AGGREGATE AS(

	SELECT 
		rental_date AS working_day,
		SUM(amount) AS total_paid,
		COUNT(customer_id) AS total_customers,
		SUM(SUM(amount)) OVER (ORDER BY DATE(rental_date)) AS running_total,
		ROUND(AVG(amount), 2) AS avg_customer_paid,
		ROUND(AVG(return_date - rental_date),2) AS avg_days_rental_duration
	FROM TEMP_ALL_PAYMENTS_TABLE 
	GROUP BY 
		rental_date
	ORDER BY 
		working_day
);
CREATE INDEX idx_rental_id_temp_table_daily ON TEMP_ALL_PAYMENTS_TABLE(rental_id);
SELECT * FROM DAILY_AGGREGATE

--3--
DROP INDEX IF EXISTS idx_working_day_temp_table;
DROP TABLE IF EXISTS TEMP_DAILY_AGGREGATE_REVENUE;
CREATE TABLE TEMP_DAILY_AGGREGATE_REVENUE AS(
	
	SELECT 
		working_day,
		total_customers,
		running_total,
		total_paid,
		COALESCE(LAG(total_paid) OVER (ORDER BY working_day),0) AS previous_day,
		(total_paid - (COALESCE (LAG(total_paid) OVER (ORDER BY working_day),0))) daily_difference,
		avg_customer_paid,
		avg_days_rental_duration
	FROM DAILY_AGGREGATE
	
);
CREATE INDEX idx_working_day_temp_table ON TEMP_DAILY_AGGREGATE_REVENUE(working_day);
SELECT * FROM TEMP_DAILY_AGGREGATE_REVENUE

--4--
DROP INDEX IF EXISTS idx_working_day_temp_table_rating;
DROP TABLE IF EXISTS TEMP_RENTAL_RATING;
CREATE TABLE TEMP_RENTAL_RATING AS (
	SELECT  
		rental_date,
		rating,
		COUNT(rental_id) AS total_rentals,
		ROW_NUMBER() OVER (PARTITION by DATE(rental_date) ORDER BY COUNT(rental_id) DESC) AS rental_rank
	FROM public.TEMP_ALL_PAYMENTS_TABLE
	GROUP BY 
		DATE(rental_date),
		rating
	ORDER BY    DATE(rental_date),
				COUNT(rental_id) DESC
);
CREATE INDEX idx_working_day_temp_table_rating ON TEMP_RENTAL_RATING(rental_date);
SELECT * FROM TEMP_RENTAL_RATING;

--5--
DROP INDEX IF EXISTS idx_rental_rank1;
DROP TABLE IF EXISTS rental_rank1;
CREATE TABLE rental_rank1 AS (
	SELECT * 
	FROM TEMP_RENTAL_RATING
	WHERE rental_rank = 1
);
CREATE INDEX idx_rental_rank1 ON TEMP_RENTAL_RATING(rental_date);
SELECT * FROM rental_rank1;

--6--
DROP INDEX IF EXISTS idx_daily_growth;
DROP TABLE IF EXISTS daily_growth;
DROP TABLE IF EXISTS percentage_of_total;
CREATE TEMPORARY TABLE percentage_of_total AS
SELECT 
    working_day,
    total_paid,
    running_total,
    avg_customer_paid,
    avg_days_rental_duration,
    CASE
        WHEN daily_difference < 0 THEN 'NEGATIVE'
        ELSE 'POSITIVE'
    END AS daily_growth,
    ROUND((total_paid / NULLIF((SELECT SUM(amount) FROM TEMP_ALL_PAYMENTS_TABLE), 0)) * 100, 2) AS of_total_percentage
FROM TEMP_DAILY_AGGREGATE_REVENUE;


CREATE TABLE daily_growth AS (
    SELECT *,
    SUM(of_total_percentage) OVER (ORDER BY working_day) AS cumulative_percentage
    FROM percentage_of_total
);

CREATE INDEX idx_daily_growth_final ON daily_growth(working_day);


DROP TABLE IF EXISTS report.agg_daily;
CREATE TABLE report.agg_daily(

	working_day date,
	total_paid numeric,
	running_total numeric,
	avg_customer_paid numeric,
	avg_days_rental_duration numeric,
	daily_growth text,
	of_total_percentage numeric,
	cumulative_percentage numeric
);

INSERT INTO report.agg_daily
(
	SELECT
		*
	FROM daily_growth

)

SELECT * FROM report.agg_daily






---------MONTHLY
--7--
DROP INDEX IF EXISTS idx_working_temp_table_monthly_agg;
DROP TABLE IF EXISTS monthly_aggregate;
CREATE TEMP TABLE monthly_aggregate AS(

	SELECT 
		RANK() OVER(ORDER BY EXTRACT(YEAR FROM rental_date), EXTRACT(MONTH FROM rental_date)) AS ranking,
		EXTRACT(YEAR FROM rental_date) AS working_year,
		EXTRACT(MONTH FROM rental_date) AS working_month,
		COUNT(customer_id) AS total_customers,
		SUM(amount) AS total_paid,
		ROUND(AVG(amount), 2) AS avg_customer_paid
	FROM TEMP_ALL_PAYMENTS_TABLE  
	GROUP BY 
		EXTRACT(YEAR FROM rental_date),
		EXTRACT(MONTH FROM rental_date)
	ORDER BY 
		EXTRACT(YEAR FROM rental_date),
		EXTRACT(MONTH FROM rental_date)
);
CREATE INDEX idx_working_temp_table_monthly_agg ON monthly_aggregate(working_month);
SELECT * FROM monthly_aggregate;

--8--
DROP INDEX IF EXISTS idx_MONTH_AGG_REVENUE;
DROP TABLE IF EXISTS monthly_agg_revenue;
CREATE TEMP TABLE monthly_agg_revenue AS(
	
	SELECT 
		*,
		COALESCE (LAG(total_paid) OVER (ORDER BY working_year,working_month),0) AS previous_month,
		(total_paid - COALESCE (LAG(total_paid) OVER (ORDER BY working_year,working_month),0)) AS monthly_difference,
		SUM(total_paid) OVER (ORDER BY ranking) AS running_total
	FROM 
		monthly_aggregate
	ORDER BY 
		ranking
);
CREATE INDEX idx_MONTH_AGG_REVENUE ON monthly_aggregate(working_month);
SELECT * FROM monthly_agg_revenue;
--9--
DROP INDEX IF EXISTS idx_working_temp_table_monthly_rating;
DROP TABLE IF EXISTS TEMP_RENTAL_RATING_MONTHLY;	
CREATE TEMP TABLE TEMP_RENTAL_RATING_MONTHLY AS (
	SELECT  
		EXTRACT(YEAR FROM TEMP_ALL_PAYMENTS_TABLE.rental_date) AS working_year,
		EXTRACT(MONTH FROM TEMP_ALL_PAYMENTS_TABLE.rental_date) AS working_month,
		se_film.rating AS rating,
		COUNT(TEMP_ALL_PAYMENTS_TABLE.rental_id) AS total_rentals
	FROM public.TEMP_ALL_PAYMENTS_TABLE
	INNER JOIN public.inventory AS se_inventory
		ON TEMP_ALL_PAYMENTS_TABLE.inventory_id = se_inventory.inventory_id
	INNER JOIN public.film AS se_film
		ON se_inventory.film_id = se_film.film_id
	GROUP BY 
		EXTRACT(YEAR FROM TEMP_ALL_PAYMENTS_TABLE.rental_date),
		EXTRACT(MONTH FROM TEMP_ALL_PAYMENTS_TABLE.rental_date),
		se_film.rating
	ORDER BY EXTRACT(MONTH FROM TEMP_ALL_PAYMENTS_TABLE.rental_date), 
			 COUNT(TEMP_ALL_PAYMENTS_TABLE.rental_id) DESC
);
CREATE INDEX idx_working_temp_table_monthly_rating ON TEMP_RENTAL_RATING_MONTHLY(working_month);
SELECT * FROM TEMP_RENTAL_RATING_MONTHLY;

--10--
DROP INDEX IF EXISTS idx_working_temp_table_monthly_rank;
DROP TABLE IF EXISTS TEMP_RATING_RANK_MONTHLY;	
CREATE TEMP TABLE TEMP_RATING_RANK_MONTHLY AS(
	SELECT 
		*, 
		ROW_NUMBER() OVER (PARTITION by working_year, working_month ORDER BY total_rentals DESC) AS rental_rank
	FROM TEMP_RENTAL_RATING_MONTHLY
);
CREATE INDEX idx_working_temp_table_monthly_rank ON TEMP_RATING_RANK_MONTHLY(working_month);
SELECT * FROM TEMP_RATING_RANK_MONTHLY;

--11--
DROP INDEX IF EXISTS idx_working_temp_table_monthly_top_3;
DROP TABLE IF EXISTS TEMP_TOP3_RATING_RENTED_MONTHLY;	
CREATE TEMP TABLE TEMP_TOP3_RATING_RENTED_MONTHLY AS (
	SELECT 
		tab1.working_month,
		tab1.rating AS top_1_rating,
		tab2.rating AS top_2_rating,
		tab3.rating AS top_3_rating
	FROM TEMP_RATING_RANK_MONTHLY AS tab1	
	INNER JOIN TEMP_RATING_RANK_MONTHLY AS tab2
		ON tab1.working_month = tab2.working_month
	INNER JOIN TEMP_RATING_RANK_MONTHLY AS tab3
		ON tab1.working_month = tab3.working_month
	WHERE tab1.rental_rank = 1 AND tab2.rental_rank = 2 AND tab3.rental_rank = 3
	GROUP BY 
		tab1.working_month,
		tab1.rating,
		tab2.rating,
		tab3.rating
);
CREATE INDEX idx_working_temp_table_monthly_top_3 ON TEMP_TOP3_RATING_RENTED_MONTHLY(working_month);
SELECT * FROM TEMP_TOP3_RATING_RENTED_MONTHLY;

--12--
DROP INDEX IF EXISTS idx_working_temp_table_monthly_aggr;
DROP TABLE IF EXISTS TEMP_MONTHLY_AGGR;	
CREATE TEMP TABLE TEMP_MONTHLY_AGGR AS(
	SELECT
		tab1.working_year,
		tab1.working_month,
		tab1.total_paid,
		tab1.previous_month,
		tab1.running_total,
		tab1.avg_customer_paid,
		tab1.monthly_difference,
		tab2.top_1_rating,
		tab2.top_2_rating,
		tab2.top_3_rating,
		CASE
			WHEN tab1.monthly_difference < 0 
			THEN 'NEGATIVE'
			ELSE 'POSITIVE'
			END AS monthly_growth
	FROM monthly_agg_revenue AS tab1  -- was revenue_lag
	INNER JOIN TEMP_TOP3_RATING_RENTED_MONTHLY AS tab2
		ON tab1.working_month = tab2.working_month
	ORDER BY
		tab1.working_year,
		tab1.working_month
);
CREATE INDEX idx_working_temp_table_monthly_aggr ON TEMP_MONTHLY_AGGR(working_month);
SELECT * FROM TEMP_MONTHLY_AGGR;

--EVERYTHING ABOVE THIS IS DONE 
------------------
DROP INDEX IF EXISTS 
DROP TABLE IF EXISTS report.agg_monthly;
CREATE TABLE report.agg_monthly(

	working_year numeric,
	working_month numeric,
	total_paid numeric,
	previous_month numeric,
	running_total numeric,
	avg_customer_paid numeric,
	monthly_difference numeric,
	top_1_rating mpaa_rating,
	top_2_rating mpaa_rating,
	top_3_rating mpaa_rating,
	monthly_growth text,
	running_total_percentage numeric
);

--insert into new table
INSERT INTO report.agg_monthly(

	SELECT *
	FROM TEMP_MONTHLY_AGGR
)

SELECT * FROM report.agg_monthly

----yearly
DROP INDEX IF EXISTS idx_working_temp_table_yearly_agg;
DROP TABLE IF EXISTS YEARLY_AGGREGATE;
CREATE TEMP TABLE YEARLY_AGGREGATE AS(

	SELECT 
		EXTRACT(YEAR FROM rental_date) AS working_year,
		COUNT(rental_id) AS total_rentals,
		SUM(amount) AS total_paid,
		SUM(SUM (amount)) OVER (ORDER BY EXTRACT(YEAR FROM rental_date)) AS running_total,
		ROUND(AVG(amount), 2) AS avg_customer_paid
	FROM TEMP_ALL_PAYMENTS_TABLE  
	GROUP BY 
		EXTRACT(YEAR FROM rental_date)
	ORDER BY 
		EXTRACT(YEAR FROM rental_date)
);
CREATE INDEX idx_working_temp_table_yearly_agg ON YEARLY_AGGREGATE(working_year);

SELECT * FROM YEARLY_AGGREGATE


DROP INDEX IF EXISTS idx_yearly_rank_rating;
DROP TABLE IF EXISTS yearly_rank_rating;
CREATE TABLE yearly_rank_rating AS (
	SELECT
    	EXTRACT(YEAR FROM rental_date) AS rental_year,
    	rating,
    	COUNT(rental_id) AS total_rentals,
    	ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM rental_date) ORDER BY COUNT(rental_id) DESC) AS rental_rank
	FROM
    	public.TEMP_ALL_PAYMENTS_TABLE
	GROUP BY
    	EXTRACT(YEAR FROM rental_date),
    	rating
	ORDER BY
    	rental_year,
    	total_rentals DESC
);
CREATE INDEX idx_yearly_rank_rating ON TEMP_RENTAL_RATING(EXTRACT(YEAR FROM rental_date));
SELECT * FROM yearly_rank_rating;

DROP INDEX IF EXISTS idx_working_temp_table_monthly_top_3;
DROP TABLE IF EXISTS rental_year;	
CREATE TEMP TABLE rental_year AS (
	SELECT 
		tab1.rental_year,
		tab1.rating AS top_1_rating,
		tab2.rating AS top_2_rating,
		tab3.rating AS top_3_rating
	FROM yearly_rank_rating AS tab1	
	INNER JOIN yearly_rank_rating AS tab2
		ON tab1.rental_year = tab2.rental_year
	INNER JOIN yearly_rank_rating AS tab3
		ON tab1.rental_year = tab3.rental_year
	WHERE tab1.rental_rank = 1 AND tab2.rental_rank = 2 AND tab3.rental_rank = 3
	GROUP BY 
		tab1.rental_year,
		tab1.rating,
		tab2.rating,
		tab3.rating
);
CREATE INDEX idx_working_temp_table_monthly_top_3 ON TEMP_TOP3_RATING_RENTED_MONTHLY(working_month);
SELECT * FROM rental_year;

-- Final year table
DROP INDEX IF EXISTS idx_final_year_table;
DROP TABLE IF EXISTS final_year_table;
CREATE TEMP TABLE final_year_table AS(
	SELECT 
		*
	FROM YEARLY_AGGREGATE AS yearly_agg
	INNER JOIN rental_year
	ON rental_year.rental_year = yearly_agg.working_year
);
CREATE INDEX idx_final_year_table ON YEARLY_AGGREGATE(working_year);

SELECT * FROM final_year_table

DROP TABLE IF EXISTS report.agg_yearly;
CREATE TABLE report.agg_yearly(

	working_year numeric,
	total_rentals bigint,
	total_paid numeric,
	running_total numeric,
	avg_customer_paid numeric,
	rental_year numeric,
	top_1_rating mpaa_rating,
	top_2_rating mpaa_rating,
	top_3_rating mpaa_rating
);

INSERT INTO report.agg_yearly(

	SELECT 
		*			
	FROM final_year_table
)












