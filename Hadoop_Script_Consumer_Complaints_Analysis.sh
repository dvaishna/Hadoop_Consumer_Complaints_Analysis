# Gitbash session - 1
# Upload Files to Linux tmp directory
# change the path of the dataset (json file)

scp C:/MSIS/CIS_5200/Project/Consumer_Complaints_Analysis/Dataset/complaints.json dvaishn2@144.24.53.159:/tmp


# Gitbash session 2

# You need to remotely access your Oracle Big Data server that you executed in your Oracle Cloud account using ssh. 
# Your CalStateLA username(dvaishn2) should be a username/password to connect to the Hadoop cluster at BDCE as follows:

ssh dvaishn2@144.24.53.159

# Downloading the dictnioary from internet
wget -O dictionary.tsv https://github.com/dalgual/aidatasci/raw/master/data/bigdata/dictionary.tsv 

# creating new directory

hdfs dfs -mkdir 5200_Complaints_DS
hdfs dfs -mkdir 5200_Project_Tables
hdfs dfs -mkdir 5200_Project_Tables/DV
hdfs dfs -mkdir 5200_Project_Tables/DV/Dictionary

# Creating separte folder for team mates

hdfs dfs -mkdir /user/dvaishn2/5200_Project_Tables/Mani
hdfs dfs -mkdir /user/dvaishn2/5200_Project_Tables/AK
hdfs dfs -mkdir /user/dvaishn2/5200_Project_Tables/MD
hdfs dfs -ls

# Display the content

hdfs dfs -ls 5200_Project_Tables/ 

# Transfer files to HDFS

hdfs dfs -put /tmp/complaints.json 5200_Complaints_DS
hdfs dfs -ls 5200_Complaints_DS/

# Putting dictionary in HDFS

hdfs dfs -put dictionary.tsv 5200_Project_Tables/DV/dictionary/ 

# Confirming the files

hdfs dfs -ls 5200_Project_Tables/DV/dictionary

# Changing the permission for the use of teammates

hdfs dfs -chmod -R o+rw /user/dvaishn2/5200_Complaints_DS/
hdfs dfs -chmod -R o+rw /user/dvaishn2/5200_Project_Tables/

# Open new gitbash session for beeline
# Gitbash session 3

# You need to remotely access your Oracle Big Data server that you executed in your Oracle Cloud account using ssh. 
# Your CalStateLA username(dvaishn2) should be a username/password to connect to the Hadoop cluster at BDCE as follows:

ssh dvaishn2@144.24.53.159

--Creating Hive Queries

beeline

--create your database
CREATE DATABASE IF NOT EXISTS dvaishn2; 

show databases;
use dvaishn2;

--External Table
 
CREATE EXTERNAL TABLE IF NOT EXISTS raw_complaints(json_response STRING)
STORED AS TEXTFILE
LOCATION '/user/dvaishn2/5200_Complaints_DS/';

show tables; 
SELECT * FROM raw_complaints LIMIT 2;

--create the base table

DROP TABLE IF EXISTS complaints;

CREATE TABLE complaints(
c_date_received string, c_product string, c_sub_product string,
c_issue string, c_sub_issue string, c_complaint_narrative string, c_company_public_response string,
c_company string, c_state string, c_zip int,
c_tags string, c_consumer_consent string, c_submitted_via string,
c_date_sent_to_company string, c_company_response_to_consumer string,
c_timely_response string, c_consumer_disputed string, c_complaint_id bigint
);

show tables;

--insert the structured data into the "complaints" table

FROM raw_complaints
INSERT INTO TABLE complaints
SELECT CAST(to_date(from_unixtime(unix_timestamp(get_json_object(json_response, '$.date_received'), 'yyyy-MM-dd'))) AS date),
get_json_object(json_response, '$.product'),
get_json_object(json_response, '$.sub_product'),
get_json_object(json_response, '$.issue'),
get_json_object(json_response, '$.sub_issue'),
get_json_object(json_response, '$.complaint_what_happened'),
get_json_object(json_response, '$.company_public_response'),
get_json_object(json_response, '$.company'),
get_json_object(json_response, '$.state'),
get_json_object(json_response, '$.zip_code'),
get_json_object(json_response, '$.tags'),
get_json_object(json_response, '$.consumer_consent_provided'),
get_json_object(json_response, '$.submitted_via'),
get_json_object(json_response, '$.date_sent_to_company'),
get_json_object(json_response, '$.company_response'),
get_json_object(json_response, '$.timely'),
get_json_object(json_response, '$.consumer_disputed'),
get_json_object(json_response, '$.complaint_id');

SELECT * FROM complaints LIMIT 2; 

--creates a Hive table company_data

DROP TABLE IF EXISTS company_data;

CREATE TABLE company_data
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES
("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/DV/company_data'
AS
SELECT c_complaint_id, c_date_received, regexp_replace(c_company,'[^a-zA-Z0-9,:\'\"`;\$\(\)\\s]', '') company
FROM complaints
GROUP BY c_complaint_id, c_date_received, c_company
HAVING year(c_date_received) BETWEEN 2019 AND 2023
ORDER BY c_complaint_id;

--Hive Query to get the total complaints

DROP TABLE IF EXISTS product_data;

CREATE TABLE product_data
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES
("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/DV/product_data'
AS
SELECT c_complaint_id, c_date_received, regexp_replace(c_product,'[^a-zA-Z0-9,:\'\"`;\$\(\)\\s]', '') product, regexp_replace(c_sub_product,'[^a-zA-Z0-9,:\'\"`;\$\(\)\\s]', '') sub_product
FROM complaints
GROUP BY c_complaint_id, c_date_received, c_product, c_sub_product
HAVING year(c_date_received) BETWEEN 2019 AND 2023
ORDER BY c_complaint_id;

--Hive query to get the total complaints

DROP TABLE IF EXISTS issue_data;

CREATE TABLE issue_data
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/DV/issue_data'
AS
SELECT c_complaint_id, c_date_received, regexp_replace(c_issue,'[^a-zA-Z0-9,:\'\"`;\$\(\)\\s]', '') issue, regexp_replace(c_sub_issue,'[^a-zA-Z0-9,:\'\"`;\$\(\)\\s]', '') sub_issue
FROM complaints
GROUP BY c_complaint_id, c_date_received, c_issue, c_sub_issue
HAVING year(c_date_received) BETWEEN 2019 AND 2023
ORDER BY c_complaint_id;

--Hive query to get top 10 issues

DROP TABLE IF EXISTS top_issues;

CREATE TABLE top_issues
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES
("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/DV/top_issues'
AS
with top_10_issues AS
(
SELECT issue, COUNT(*) AS complaints_count
FROM issue_data
GROUP BY issue
ORDER BY complaints_count DESC
LIMIT 10)
select * from top_10_issues
ORDER BY complaints_count DESC;

SELECT * FROM top_issues LIMIT 3;

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS: 
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/DV/top_issues/00000*_0

# Prepare the file(s) in Linux to download to local PC: 
ls -al 

# concatenate the csv files (if required)
cat 000000_0 > top_issues.csv 

# remove this file
rm 000000_0

# Copy the file to local PC: 
scp dvaishn2@144.24.53.159:/home/dvaishn2/top_issues.csv top_issues.csv

--Hive Query to get the number of complaints received by CFPB for the year 2019 to 2023

DROP TABLE IF EXISTS complaints_date_analysis;

CREATE TABLE complaints_date_analysis
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES
("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/MD/complaints_date_analysis'
AS
SELECT c_date_received ,COUNT(c_complaint_id)
FROM dvaishn2.complaints
GROUP BY  c_date_received
Having (YEAR(c_date_received) BETWEEN 2021 AND 2023) AND  c_date_received IS NOT NULL
ORDER BY c_date_received ;

select * from complaints_date_analysis limit 10;

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS: 
hdfs dfs -get /user/mdhoke/5200_Project_Tables/MD/complaints_date_analysis/00000*_0

# Prepare the file(s) in Linux to download to local PC:
ls -al 

# concatenate the csv files (if required)
cat 000000_0 > complaints_date_analysis.csv 

# remove this file from the Linux
rm 000000_0 

# Copy the file to local PC: 
scp mdhoke@144.24.53.159:/home/mdhoke/complaints_date_analysis.csv complaints_date_analysis.csv

--Display companies with their Product and respective issues for each state

DROP TABLE IF EXISTS state_table;

CREATE TABLE state_table row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/Mani/state_table'
AS
SELECT cd.c_complaint_id AS complaint_id, cd.c_date_received AS date_received, cd.company AS company,
pd.product AS Product, id.issue AS issue, master.c_state AS state, master.c_zip AS zip
FROM dvaishn2.complaints master
JOIN dvaishn2.company_data cd ON cd.c_complaint_id =  master.c_complaint_id
JOIN dvaishn2.product_data pd ON pd.c_complaint_id = master.c_complaint_id
JOIN dvaishn2.issue_data id ON id.c_complaint_id= master.c_complaint_id
GROUP BY cd.c_complaint_id, cd.c_date_received, cd.company, pd.product, id.issue, master.c_state, master.c_zip
HAVING year(cd.c_date_received) BETWEEN 2021 AND 2023 AND master.c_state != ''
ORDER BY state;

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS:
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/Mani/state_table/00000*_0

# Prepare the file(s) in Linux to download to local PC:
ls -al
 
# concatenate the csv files (if required)
cat 000000_0 > state_table.csv 

# remove this file from the Linux
rm 000000_0 

# Copy the file to local PC:  
scp mneethi@144.24.53.159:/home/mneethi/state_table.csv state_table.csv

SELECT count(*) AS total_complaints, state FROM state_table GROUP BY state ORDER BY total_complaints desc limit 10;

SELECT count(*) AS total_complaints, state FROM state_table GROUP BY state ORDER BY total_complaints asc limit 10;

--Find out top 3 products the consumers identified in the complaint

DROP TABLE IF EXISTS top_products;

CREATE TABLE top_products
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES
("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/Mani/top_products'
AS
with top_3_products AS
(SELECT product, COUNT(*) AS complaints_count
FROM dvaishn2.product_data
GROUP BY product
ORDER BY complaints_count DESC
LIMIT 3)
SELECT * FROM top_3_products ORDER BY complaints_count DESC;

SELECT * FROM top_products;

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS:
hdfs dfs -ls /user/dvaishn2/5200_Project_Tables/Mani/top_products

hdfs dfs -get /user/dvaishn2/5200_Project_Tables/Mani/top_products/00000*_0

# Download the file(s) from Linux to local PC
ls -al
 
# concatenate the csv files (if required)
cat 000000_0 > top_products.csv 

# remove this file
rm 000000_0

# Copy the file to local PC:  
scp mneethi@144.24.53.159:/home/mneethi/top_products.csv top_products.csv 

--Find out products(count) for CA:

DROP TABLE IF EXISTS ca_product;

CREATE TABLE ca_product
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/Mani/ca_product'
AS
SELECT state, count(*) AS total, product
FROM state_table
GROUP BY product, state
HAVING state = 'CA'
ORDER BY total DESC;

SELECT * FROM ca_product; 

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS: 
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/Mani/ca_product/00000*_0 

ls -al

# concatenate the csv files (if required) 
cat 000000_0 > ca_product.csv

# remove this file
rm 000000_0 

# Copy the file to local PC: 
scp mneethi@144.24.53.159:/home/mneethi/ca_product.csv ca_product.csv

--Find out top company for CA

DROP TABLE IF EXISTS ca_company;

CREATE TABLE ca_company
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/Mani/ca_company'
AS
SELECT company AS company, count(complaint_id) AS total
FROM state_table
WHERE state = 'CA'
GROUP BY company, state
HAVING company!=''
ORDER BY total DESC LIMIT 5;

SELECT * FROM ca_company;

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS:
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/Mani/ca_company/00000*_0 

# Download the file(s) from Linux to local PC: 
ls -al

# concatenate the csv files (if required)
cat 000000_0 > ca_company.csv

# Copy the file to local PC:
scp mneethi@144.24.53.159:/home/mneethi/ca_company.csv ca_company.csv

--Hive query to show the analysis on the company response to the complaints for last 3 years for the state CA
 
DROP TABLE IF EXISTS CA_company_response;

CREATE TABLE CA_company_response
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES
("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/DV/CA_company_response'
AS
SELECT c_company, c_company_response_to_consumer as company_response
FROM mneethi.top_products tp, top_issues ti, complaints
WHERE tp.product = 'Credit reporting, credit repair services, or other personal consumer reports'
AND ti.issue = 'Incorrect information on your report'
AND c_company = 'EQUIFAX, INC.'
AND c_state = 'CA'
AND c_company_response_to_consumer != ''
AND YEAR(c_date_received) BETWEEN 2021 AND 2023; 

select distinct c_company, company_response from CA_company_response; 


# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS:
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/DV/CA_company_response/00000*_0 

# Prepare the file(s) in Linux to download to local PC: 
ls -al

# concatenate the csv files (if required)
cat 000000_0 > CA_company_response.csv 

# remove this file
rm 000000_0 

# Copy the file to local PC:
scp dvaishn2@144.24.53.159:/home/dvaishn2/CA_company_response.csv CA_company_response.csv

--Find out the top 5 companies which have received the highest number of complaints

CREATE TABLE top_companies_complaints
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES
("separatorChar" = ",","quoteChar" = "\"")
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/AK/top_companies_complaints'
AS
with top_companies_complaints AS
(
SELECT company, count(*) as complaints FROM dvaishn2. company_data master GROUP BY company ORDER BY complaints DESC LIMIT 5)
select * FROM top_companies_complaints ORDER BY complaints DESC;

select * from top_companies_complaints;

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS:
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/AK/top_companies_complaints/00000*_0

# Prepare the file(s) in Linux to download to local PC: 
ls -al
cat 000000_0 > top_companies_complaints.csv
ls -al

# remove this file
rm 000000_0 

# Copy the file to local PC:
scp akhaire3@144.24.53.159:/home/akhaire3/top_companies_complaints.csv top_companies_complaints.csv

--How did the consumer submit the complaint to the CFPB? 

CREATE TABLE complaints_submitted_via
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
STORED AS TEXTFILE
LOCATION '/user/dvaishn2/5200_Project_Tables/AK/complaints_submitted_via'
AS
SELECT c_state, c_submitted_via, COUNT(c_complaint_id) AS complaints
FROM dvaishn2.complaints
GROUP BY c_state, c_submitted_via
HAVING c_state!='' AND c_submitted_via!='';

select * from complaints_submitted_via;

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS:
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/AK/complaints_submitted_via/00000*_0

# Prepare the file(s) in Linux to download to local PC:
ls -al
cat 000000_0 > complaints_submitted_via.csv
ls -al

# remove this file
rm 000000_0 

# Copy the file to local PC:
scp akhaire3@144.24.53.159:/home/akhaire3/complaints_submitted_via.csv complaints_submitted_via.csv

--Sentiment Analysis on Complaints Narrative

CREATE EXTERNAL TABLE dictionary (
type string,
length int,
word string,
pos string,
stemmed string,
polarity string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/dvaishn2/5200_Project_Tables/DV/dictionary';

SELECT * FROM dictionary LIMIT 5;

--This query creates a view complaints_clean.

CREATE VIEW IF NOT EXISTS complaints_clean AS
SELECT
c_complaint_id,
c_date_received,
regexp_replace(c_complaint_narrative, '[^a-zA-Z0-9\\s]', '') narrative,
c_state
FROM complaints
WHERE YEAR(c_date_received) BETWEEN 2022 AND 2023;

--views will split the consumer narrative value to each word and explode in multiple rows:

CREATE VIEW IF NOT EXISTS complaints_v1 as
SELECT c_complaint_id, c_date_received, words
FROM complaints
LATERAL VIEW EXPLODE(sentences(lower(c_complaint_narrative))) dummy as words
WHERE YEAR(c_date_received) BETWEEN 2022 AND 2023;


CREATE VIEW IF NOT EXISTS complaints_v2 as
SELECT c_complaint_id, c_date_received, word
FROM complaints_v1
LATERAL VIEW EXPLODE ( words ) dummy as word
WHERE YEAR(c_date_received) BETWEEN 2022 AND 2023;

-- view is created to map the polarity value to each word of complaint narrative:

create view IF NOT EXISTS complaints_v3 as select
c_complaint_id, c_date_received,
complaints_v2.word,
case d.polarity
when 'negative' then -1
when 'positive' then 1
else 0 end as polarity
from complaints_v2 left outer join dictionary d on complaints_v2.word = d.word
WHERE YEAR(c_date_received) BETWEEN 2022 AND 2023
ORDER BY polarity ASC;

--calculates the overall sentiment for each consumer narrative for each complaint id:

create table IF NOT EXISTS complaint_sentiment
stored as orc as
SELECT c_complaint_id, c_date_received,
case
when sum( polarity ) > 0 then 'positive'
when sum( polarity ) < 0 then 'negative'
else 'neutral' end as sentiment
from complaints_v3
group by c_complaint_id, c_date_received, YEAR(c_date_received)
HAVING YEAR(c_date_received) BETWEEN 2022 AND 2023;

--query assigns values to polarity for proper computation of the data: 

CREATE TABLE IF NOT EXISTS complaints_bi
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/user/dvaishn2/5200_Project_Tables/DV/complaints_bi"
AS SELECT
cc.*,
case cs.sentiment
when 'positive' then 2
when 'neutral' then 1
when 'negative' then 0
end as sentiment
FROM complaints_clean cc LEFT OUTER JOIN complaint_sentiment cs
on cc.c_complaint_id = cs.c_complaint_id
WHERE YEAR(c_date_received) BETWEEN 2022 AND 2023;

--final sentiment table containing the clean complaints narrative & sentiment values:

CREATE TABLE IF NOT EXISTS complaints_sentiment
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/user/dvaishn2/5200_Project_Tables/DV/complaints_sentiment"
AS
select c_complaint_id, c_date_received, narrative, c_state, sentiment from complaints_bi
where c_complaint_id IS NOT NULL
AND ( sentiment =0 or sentiment =1 or sentiment =2)
AND (YEAR(c_date_received) BETWEEN 2022 AND 2023);

# Execute these commands in session 1 (not in beeline) to get the file on Linux from HDFS:
hdfs dfs -get /user/dvaishn2/5200_Project_Tables/DV/complaints_sentiment/000*_0

# Prepare the file(s) in Linux to download to local PC:
ls -al

# concatenate the csv files (if required)
cat 00000{0..9}_0 00001{0..9}_0 00002{0..4}_0 > senti_out.csv

# remove this file from the Linux
rm 00*_0 

# Copy the file to local PC:
scp dvaishn2@144.24.53.159:/home/dvaishn2/senti_out.csv senti_out.csv

--NGram Sentiment analysis

DROP TABLE IF EXISTS raw_narrative;
CREATE TABLE raw_narrative
(
product string,
issue string,
company string,
narrative string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
STORED AS TEXTFILE LOCATION '/user/dvaishn2/5200_Project_Tables/DV/raw_narrative';

INSERT OVERWRITE table raw_narrative
SELECT CONCAT('{ "product": "', tp.product, '"}'), CONCAT('{"issue": "', ti.issue, '"}'), CONCAT('{"company": "', c_company, '"}'), CONCAT('{"narrative": "', regexp_replace(c_complaint_narrative,'[xX]', ''), '"}')
FROM mneethi.top_products tp, top_issues ti, complaints
WHERE tp.product = 'Credit reporting, credit repair services, or other personal consumer reports' AND ti.issue = 'Incorrect information on your report' AND c_company = 'EQUIFAX, INC.';

SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(narrative)), 4, 10))
AS snippet
FROM raw_narrative;

 
 




 
