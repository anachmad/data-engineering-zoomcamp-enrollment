-- Create the external table from fhv tripdata 2019
CREATE OR REPLACE EXTERNAL TABLE `de-course-376622.de_course_dataset.external_fhv_tripdata_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://de-course-376622-bucket/data\\fhv\\fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE `de-course-376622.de_course_dataset.fhv_tripdata_2019_unpartitioned` as
SELECT * FROM de_course_dataset.external_fhv_tripdata_2019

-- Q1
SELECT COUNT(*) FROM `de_course_dataset.external_fhv_tripdata_2019`;

-- Q2
SELECT DISTINCT(Affiliated_base_number) FROM de_course_dataset.fhv_tripdata_2019_unpartitioned;
SELECT DISTINCT(Affiliated_base_number) FROM de_course_dataset.external_fhv_tripdata_2019;

-- Q3
SELECT COUNT(*) FROM de_course_dataset.fhv_tripdata_2019_unpartitioned where PULocationID is null and DOLocationID is null;