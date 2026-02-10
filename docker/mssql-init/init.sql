CREATE DATABASE panopticum_dev;
GO
USE panopticum_dev;
GO
CREATE SCHEMA test;
GO
CREATE TABLE test.users (id INT PRIMARY KEY, name NVARCHAR(100));
CREATE TABLE test.products (id INT PRIMARY KEY, title NVARCHAR(200), price DECIMAL(10,2));
INSERT INTO test.users VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO test.products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99);
GO
