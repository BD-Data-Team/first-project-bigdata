-- drop table if exists reviews;

-- CREATE TABLE if not exists reviews (
--   id INT,
--   product_id STRING,
--   user_id STRING,
--   profile_name STRING,
--   helpfulness_numerator FLOAT,
--   helpfulness_denominator FLOAT,
--   score FLOAT,
--   time DATE,
--   summary STRING,
--   text STRING
-- ) row format delimited fields terminated BY ',' lines terminated BY '\n' 
-- tblproperties("skip.header.line.count"="1"); 

-- LOAD DATA INPATH 'hdfs:///user/data-team/input/dataset.csv' INTO TABLE reviews;


-- compute number of reviews per year and product
CREATE TABLE if not exists reviews_per_year AS
SELECT YEAR(time) as reviews_year, product_id, COUNT(*) as cnt
FROM reviews
GROUP BY YEAR(time), product_id;


-- select top 10 products per year
CREATE TABLE if not exists top_10_products_for_year AS
SELECT reviews_year, product_id
FROM (
        SELECT *, row_number() OVER (PARTITION BY reviews_year ORDER BY cnt DESC) as row_num
        FROM reviews_per_year 
    ) as ranked_reviews_per_year
WHERE row_num <= 10; 

CREATE TABLE if not exists top_10_products_for_year_with_reviews AS
SELECT top_10_products_for_year.reviews_year as reviews_year, top_10_products_for_year.product_id as product_id, reviews.text as text
FROM top_10_products_for_year, reviews
WHERE top_10_products_for_year.reviews_year = YEAR(reviews.time) AND top_10_products_for_year.product_id = reviews.product_id;

CREATE TABLE if not exists year_for_word_2_count AS
SELECT reviews_year, exploded_text.word as word, COUNT(*) as cnt
FROM top_10_products_for_year_with_reviews 
LATERAL VIEW explode(split(text, ' ')) exploded_text AS word
WHERE length(exploded_text.word) >= 4
GROUP BY reviews_year, exploded_text.word;

CREATE TABLE if not exists product_2_words AS
SELECT product_id, reviews_year, exploded_text.word as word
FROM top_10_products_for_year_with_reviews 
LATERAL VIEW explode(split(text, ' ')) exploded_text AS word;
















-- INSERT OVERWRITE DIRECTORY 'hdfs:///user/data-team/output/1_task/hive'
-- SELECT *
-- FROM (
--     SELECT reviews_year, product_id
--     FROM (
--         -- compute rank per year
--         SELECT *, row_number() OVER (PARTITION BY reviews_year ORDER BY cnt DESC) as row_num
--         FROM (
--             -- compute number of reviews per year and product
--             SELECT YEAR(time) as reviews_year, product_id, COUNT(*) as cnt
--             FROM reviews
--             GROUP BY YEAR(time), product_id
--         ) as reviews_per_year
--     ) as ranked_reviews_per_year
--     WHERE row_num <= 10) as top_10_products_for_year 
-- JOIN reviews ON top_10_products_for_year.reviews_year = YEAR(reviews.time) AND top_10_products_for_year.product_id = reviews.product_id;