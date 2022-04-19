create extension plpython3u;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS weather CASCADE;
DROP TABLE IF EXISTS weather_output;
DROP TABLE IF EXISTS weather_output_t;
DROP TABLE IF EXISTS basic;
DROP TABLE IF EXISTS basic_output;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (id int);
INSERT into t1 (id) SELECT * FROM generate_series(1,10);
CREATE TABLE t2 (id int, name int);
INSERT INTO t2 (id, name)
(WITH numbers AS (
  SELECT *
  FROM generate_series(1, 5)
)
SELECT generate_series, generate_series +2 FROM numbers);
CREATE TABLE employee (name text, payment int);
INSERT into employee (name, payment) values ('John', 1000), ('Joe', 2000), ('Jason', 3000);
CREATE TABLE weather (id int, city text, wdate timestamp, temp int, humidity int, aqi int);
CREATE TABLE weather_output (id int, city text, wdate timestamp, temp int, humidity int, aqi int);
CREATE TABLE weather_output_t (id int, city text, wdate timestamp, temp int, humidity int, aqi int);
CREATE TABLE basic (a int4, b int4);
CREATE TABLE basic_output (a int4, b int4);
INSERT INTO basic VALUES(1, 3), (2,4);
INSERT INTO weather VALUES (1, 'New York', '1970-01-01:00:00:00Z', 19, 23, 121);
INSERT INTO weather VALUES (2, 'London', '1970-01-01:00:00:00Z', 23, 21, 312);
DROP SCHEMA IF EXISTS test_Schema CASCADE;
DROP SCHEMA IF EXISTS "test_Schema" CASCADE;
CREATE Schema "test_Schema";
CREATE SCHEMA test_Schema;
