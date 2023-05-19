drop table if exists reviews;
CREATE TABLE if not exists reviews (
  id INT,
  product_id STRING,
  user_id STRING,
  profile_name STRING,
  helpfulness_numerator INT,
  helpfulness_denominator INT,
  score INT,
  time BIGINT,
  summary STRING,
  text STRING
) row format delimited fields terminated BY ',' lines terminated BY '\n' 
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '${hiveconf:dataset}' INTO TABLE reviews;

-- compute number of reviews per year and product
drop table if exists reviews_per_year;
CREATE TABLE if not exists reviews_per_year AS
SELECT YEAR(from_unixtime(time)) as reviews_year, product_id, COUNT(*) as reviews_count
FROM reviews
GROUP BY YEAR(from_unixtime(time)), product_id;

-- select top 10 products per year
drop table if exists top_10_products_for_year;
CREATE TABLE if not exists top_10_products_for_year AS
SELECT reviews_year, product_id
FROM (
        SELECT *, row_number() OVER (PARTITION BY reviews_year ORDER BY reviews_count DESC) as row_num
        FROM reviews_per_year 
    ) as ranked_reviews_per_year
WHERE row_num <= 10; 

drop table if exists top_10_products_for_year_with_reviews;
CREATE TABLE if not exists top_10_products_for_year_with_reviews AS
SELECT top_10_products_for_year.reviews_year as reviews_year, top_10_products_for_year.product_id as product_id, reviews.text as text
FROM top_10_products_for_year, reviews
WHERE top_10_products_for_year.reviews_year = YEAR(from_unixtime(time)) AND top_10_products_for_year.product_id = reviews.product_id;

drop table if exists year_for_product_2_word_count;
CREATE TABLE if not exists year_for_product_2_word_count AS
SELECT reviews_year, product_id, exploded_text.word as word, COUNT(*) as word_count
FROM top_10_products_for_year_with_reviews 
LATERAL VIEW explode(split(text, ' ')) exploded_text AS word
WHERE length(exploded_text.word) >= 4
GROUP BY reviews_year, product_id, exploded_text.word;

INSERT OVERWRITE DIRECTORY '${hiveconf:output_dir}'
SELECT reviews_year, product_id, word, word_count
FROM (  SELECT *, row_number() OVER (PARTITION BY reviews_year, product_id ORDER BY word_count DESC) as row_num
        FROM year_for_product_2_word_count 
    ) as ranked_year_for_product_2_word_count
WHERE row_num <= 5;