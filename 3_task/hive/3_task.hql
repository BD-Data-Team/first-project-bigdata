-- https://github.com/klout/brickhouse/tree/8fce0ac98aef422772ac89de7a620caac47ccc9d
CREATE TEMPORARY FUNCTION array_union AS 'brickhouse.udf.collect.ArrayUnionUDF';
CREATE TEMPORARY FUNCTION array_intersect AS 'brickhouse.udf.collect.ArrayIntersectUDF';

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
LOAD DATA INPATH 'hdfs:///user/data-team/input/dataset.csv' INTO TABLE reviews;


drop table if exists rated_products;
CREATE TABLE if not exists rated_products as 
SELECT user_id, product_id
FROM reviews
WHERE score >= 4
GROUP BY user_id, product_id;

drop table if exists product_2_users;
CREATE TABLE if not exists product_2_users as
SELECT array(product_id) as products, collect_set(user_id) as users
FROM rated_products
GROUP BY product_id
HAVING count(*) >= 3 AND product_id IS NOT NULL;

SET sethive.strict.checks.cartesian.product = false;
SET hive.mapred.mode = "nonstrict";

drop table if exists product_2_users_1;
CREATE TABLE if not exists  product_2_users_1 as
SELECT sort_array(array_union(p1.products, p2.products)) as products, array_intersect(p1.users, p2.users) as users
FROM product_2_users p1 CROSS JOIN product_2_users p2
WHERE p1.products[0] < p2.products[0] AND size(array_intersect(p1.users, p2.users)) >= 3;


drop table if exists product_2_users_2;
CREATE TABLE if not exists  product_2_users_2 as
SELECT sort_array(array_union(p1.products, p2.products)) as products, array_intersect(p1.users, p2.users) as users
FROM product_2_users_1 p1 CROSS JOIN product_2_users_1 p2
WHERE p1.products[0] < p2.products[0] AND size(array_intersect(p1.users, p2.users)) >= 3;

drop table if exists products_2_users_sorted;
CREATE TABLE if not exists  products_2_users_sorted as
SELECT distinct(sort_array(products)) as products, sort_array(users) as users
FROM product_2_users_2
ORDER BY users[0] DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///user/data-team/output/3_task/hive' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' -> ' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' lines terminated BY '\n' 
SELECT * FROM products_2_users_sorted;



-- codice per fare unione ed intersezione in un solo passaggio se non si usa brickhouseUDF
-- drop table if exists product_2_users_pairs_filtered;
-- CREATE TABLE if not exists  product_2_users_pairs_filtered as
-- SELECT products, collect_set(user1) 
-- FROM (  
--   SELECT products, user1, user2
--   FROM (  
--     SELECT products, user1, users2
--     FROM (
--       SELECT collect_set(product) as products, users1, users2
--       FROM (
--         SELECT product1 as product, users1, users2
--         FROM product_2_users_pairs
--         LATERAL VIEW explode(products1) exploded_products1 as product1 
--         UNION
--         SELECT product2 as product, users1, users2
--         FROM product_2_users_pairs
--         LATERAL VIEW explode(products2) exploded_products2 as product2
--       ) t
--       GROUP BY users1, users2 
--     ) t
--     LATERAL VIEW explode(users1) exploded_users1 as user1 
--   ) t
--   LATERAL VIEW explode(users2) exploded_users2 as user2 
-- ) t
-- WHERE products IS NOT NULL AND user1 IS NOT NULL AND user2 IS NOT NULL AND user1 == user2
-- GROUP BY products HAVING count(*) >= 3;
