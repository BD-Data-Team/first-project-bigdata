drop table if exists reviews;
-- !hdfs dfs -mkdir ${hiveconf:input_dir}/copy/;
-- !hdfs dfs -cp ${hiveconf:dataset} ${hiveconf:input_dir}/copy/;
CREATE TABLE  reviews (
  id INT,
  product_id STRING,
  user_id STRING,
  profile_name STRING,
  helpfulness_numerator INT,
  helpfulness_denominator INT,
  score INT,
  review_year INT,
  summary STRING,
  text STRING
) row format delimited fields terminated BY ',' lines terminated BY '\n' 
-- location '${hiveconf:input_dir}/copy/'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '${hiveconf:dataset}' INTO TABLE reviews;

-- compute number of reviews per year and product
drop table if exists reviews_per_year;
CREATE TABLE if not exists reviews_per_year AS
SELECT review_year, product_id, collect_list(text) as texts, COUNT(*) as reviews_count
FROM reviews
GROUP BY review_year, product_id;

-- select top 10 products per year
drop table if exists top_10_products_for_year;
CREATE TABLE if not exists top_10_products_for_year AS
SELECT review_year, product_id, texts
FROM (
        SELECT *, row_number() OVER (PARTITION BY review_year ORDER BY reviews_count DESC) as row_num
        FROM reviews_per_year 
    ) as ranked_reviews_per_year
WHERE row_num <= 10; 

drop table if exists year_for_product_2_word_count;
CREATE TABLE if not exists year_for_product_2_word_count AS
SELECT review_year, product_id, exploded_text.word as word, COUNT(*) as word_count
FROM (
  SELECT review_year, product_id, exploded_texts.text
  FROM top_10_products_for_year
  LATERAL VIEW explode(texts) exploded_texts AS text
) AS t
LATERAL VIEW explode(split(text, ' ')) exploded_text AS word
WHERE length(exploded_text.word) >= 4
GROUP BY review_year, product_id, exploded_text.word;

INSERT OVERWRITE DIRECTORY '${hiveconf:output_dir}'
SELECT review_year, product_id, word, word_count
FROM (  SELECT *, row_number() OVER (PARTITION BY review_year, product_id ORDER BY word_count DESC) as row_num
        FROM year_for_product_2_word_count 
    ) as ranked_year_for_product_2_word_count
WHERE row_num <= 5;