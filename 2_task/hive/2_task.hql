--drop table reviews;

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
)
row format delimited fields terminated BY ',' lines terminated BY '\n' 
tblproperties("skip.header.line.count"="1"); 

--LOAD DATA INPATH 'hdfs:///user/francesco/input/dataset.csv' INTO TABLE reviews;

INSERT OVERWRITE DIRECTORY 'hdfs:///user/francesco/output/2_task'
SELECT user_id, AVG(helpfulness_numerator/helpfulness_denominator) as appreciation
FROM reviews
WHERE helpfulness_denominator > 0
GROUP BY user_id
ORDER BY appreciation DESC;


--TODO: dobbiamo capire perchè ci sono i valori null alla fine del file di output
