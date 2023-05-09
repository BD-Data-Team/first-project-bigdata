-- drop table reviews;

CREATE TABLE if not exists reviews (
  id INT,
  product_id STRING,
  user_id STRING,
  profile_name STRING,
  helpfulness_numerator FLOAT,
  helpfulness_denominator FLOAT,
  score FLOAT,
  time DATE,
  summary STRING,
  text STRING
);
-- row format delimited fields terminated BY ',' lines terminated BY '\n' 
-- tblproperties("skip.header.line.count"="1"); 
-- LOAD DATA INPATH 'hdfs:///user/data-team/input/dataset.csv' INTO TABLE reviews;

-- SELECT * FROM reviews LIMIT 10;
SELECT product_id, YEAR(time) as year, COUNT(*) as cnt, text
FROM reviews
GROUP BY product_id, YEAR(time), text;



