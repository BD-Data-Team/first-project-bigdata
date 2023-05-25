
-- https://github.com/klout/brickhouse/tree/8fce0ac98aef422772ac89de7a620caac47ccc9d
CREATE TEMPORARY FUNCTION array_union AS 'brickhouse.udf.collect.ArrayUnionUDF';
CREATE TEMPORARY FUNCTION array_intersect AS 'brickhouse.udf.collect.ArrayIntersectUDF';

drop table if exists reviews;
-- !hdfs dfs -mkdir ${hiveconf:input_dir}/copy/;
-- !hdfs dfs -cp ${hiveconf:dataset} ${hiveconf:input_dir}/copy/;
CREATE TABLE if not exists reviews (
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

-- LOAD DATA INPATH '${hiveconf:dataset}' INTO TABLE reviews;

drop table if exists rated_products;
CREATE TABLE if not exists rated_products as
SELECT product_id, user_id
FROM reviews
WHERE score >= 4;


drop table if exists product_2_users;
CREATE TABLE if not exists product_2_users as
SELECT array(product_id) as products, collect_set(user_id) as users
FROM rated_products
GROUP BY product_id
HAVING count(*) > 1 AND product_id IS NOT NULL;

SET sethive.strict.checks.cartesian.product = false;
SET hive.mapred.mode = "nonstrict";

drop table if exists product_2_users_1;
CREATE TABLE if not exists  product_2_users_1 as
SELECT sort_array(array_union(p1.products, p2.products)) as products, sort_array(array_intersect(p1.users, p2.users)) as users
FROM product_2_users p1 CROSS JOIN product_2_users p2
WHERE p1.products[0] < p2.products[0] AND size(array_intersect(p1.users, p2.users)) >= 2;


drop table if exists product_2_users_2;
CREATE TABLE if not exists  product_2_users_2 as
SELECT sort_array(array_union(p1.products, p2.products)) as products, sort_array(array_intersect(p1.users, p2.users)) as users
FROM product_2_users_1 p1 CROSS JOIN product_2_users_1 p2
WHERE p1.products[0] < p2.products[0] AND size(array_intersect(p1.users, p2.users)) >= 2;

drop table if exists products_2_users_sorted;
CREATE TABLE if not exists  products_2_users_sorted as
SELECT distinct(products), users
FROM product_2_users_2
ORDER BY users[0] ASC;

INSERT OVERWRITE DIRECTORY '${hiveconf:output_dir}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' -> ' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' lines terminated BY '\n' 
SELECT * FROM products_2_users_sorted;
