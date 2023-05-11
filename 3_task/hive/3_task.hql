-- drop table if exists reviews;
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


-- LOAD DATA INPATH 'hdfs:///user/data-team/input/dataset.csv' INTO TABLE reviews;

INSERT OVERWRITE DIRECTORY 'hdfs:///user/data-team/output/3_task/hive'

SELECT collect_set(user_id) as user_ids, collect_set(product_id) as shared_products
FROM (
  SELECT user_id, product_id
  FROM reviews
  WHERE score >= 4
  GROUP BY user_id, product_id
) as rated_products
GROUP BY product_id
HAVING count(*) >= 3
ORDER BY user_ids[0];