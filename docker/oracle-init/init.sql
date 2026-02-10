CREATE TABLE users (
  id NUMBER PRIMARY KEY,
  name VARCHAR2(100)
);

CREATE TABLE products (
  id NUMBER PRIMARY KEY,
  title VARCHAR2(200),
  price NUMBER(10,2)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO products VALUES (1, 'Widget', 9.99);
INSERT INTO products VALUES (2, 'Gadget', 19.99);

COMMIT;
