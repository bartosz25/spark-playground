CREATE SCHEMA wfc;

CREATE TABLE wfc.orders (
    order_id INT PRIMARY KEY,
    customer_id VARCHAR(25)
);

CREATE TABLE wfc.order_items (
    item_id INT PRIMARY KEY,
    order_id INT,
    price INT
);
INSERT INTO wfc.orders (order_id, customer_id)
VALUES
    (1, 'user 10'),
    (2, 'user 200'),
    (3, 'user 3000');

INSERT INTO wfc.order_items (item_id, order_id, price)
VALUES
    (101, 1, 10),
    (102, 1, 20),
    (103, 1, 30),
    (204, 2, 100),
    (205, 2, 200),
    (306, 3, 3000);