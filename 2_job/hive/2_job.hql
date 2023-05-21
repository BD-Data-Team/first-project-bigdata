!hdfs dfs -cp ${hiveconf:dataset} ${hiveconf:input_dir}/copy/;
drop table if exists reviews;
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
location '${hiveconf:input_dir}/copy/'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '${hiveconf:dataset}' INTO TABLE reviews;

INSERT OVERWRITE DIRECTORY '${hiveconf:output_dir}'
SELECT user_id, AVG(helpfulness_numerator / helpfulness_denominator) as appreciation
FROM reviews
WHERE NOT (helpfulness_numerator > helpfulness_denominator OR helpfulness_denominator <= 0.0)
GROUP BY user_id
ORDER BY appreciation DESC;
drop table if exists reviews;