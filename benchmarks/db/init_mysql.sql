SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS departments;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE departments (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL UNIQUE,
    cost_center VARCHAR(20)
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    department_id INTEGER,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    title VARCHAR(120),
    hired_at DATE NOT NULL,
    salary DECIMAL(12,2) NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE customers (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    state VARCHAR(50) NOT NULL,
    city VARCHAR(80) NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    category VARCHAR(80) NOT NULL,
    price DECIMAL(12,2) NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    customer_id INTEGER,
    order_date DATE NOT NULL,
    status VARCHAR(40) NOT NULL,
    shipping_address VARCHAR(200) NOT NULL,
    total DECIMAL(12,2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE invoices (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    customer_id INTEGER,
    order_id INTEGER,
    status VARCHAR(40) NOT NULL,
    total DECIMAL(12,2) NOT NULL,
    issued_at DATE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE payments (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    invoice_id INTEGER,
    amount DECIMAL(12,2) NOT NULL,
    paid_at DATE NOT NULL,
    method VARCHAR(40) NOT NULL,
    FOREIGN KEY (invoice_id) REFERENCES invoices(id)
);

CREATE TABLE sales (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    employee_id INTEGER,
    amount DECIMAL(12,2) NOT NULL,
    sale_date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(id)
);

INSERT INTO departments (name, cost_center) VALUES
('Engineering', 'CC-100'), ('Sales', 'CC-200'), ('Finance', 'CC-300');

INSERT INTO employees (department_id, first_name, last_name, title, hired_at, salary) VALUES
(1, 'Ava', 'Shaw', 'Software Engineer', '2022-03-01', 125000.00),
(2, 'Liam', 'Patel', 'Account Executive', '2021-08-15', 98000.00),
(3, 'Noah', 'Kim', 'Financial Analyst', '2020-01-10', 110000.00);

INSERT INTO customers (name, state, city) VALUES
('Acme Corp', 'California', 'San Francisco'),
('Northwind LLC', 'New York', 'Buffalo'),
('Bluebird Inc', 'California', 'San Diego');

INSERT INTO products (name, category, price) VALUES
('Analytics Suite', 'Software', 250.00),
('Support Plan', 'Service', 75.00),
('Data Connector', 'Software', 125.00);

INSERT INTO orders (customer_id, order_date, status, shipping_address, total) VALUES
(1, '2026-01-10', 'paid', '1 Market St', 625.00),
(2, '2026-02-11', 'open', '42 Lake Ave', 250.00),
(3, '2026-03-12', 'paid', '9 Harbor Dr', 325.00);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 2, 250.00), (1, 3, 1, 125.00), (2, 1, 1, 250.00), (3, 2, 1, 75.00), (3, 3, 2, 125.00);

INSERT INTO invoices (customer_id, order_id, status, total, issued_at) VALUES
(1, 1, 'paid', 625.00, '2026-01-11'), (2, 2, 'open', 250.00, '2026-02-12'), (3, 3, 'paid', 325.00, '2026-03-13');

INSERT INTO payments (invoice_id, amount, paid_at, method) VALUES
(1, 625.00, '2026-01-15', 'card'), (3, 325.00, '2026-03-15', 'ach');

INSERT INTO sales (employee_id, amount, sale_date, region, channel) VALUES
(2, 25000.00, '2026-01-15', 'North America', 'Direct'),
(2, 18000.00, '2026-02-20', 'North America', 'Partner'),
(2, 32000.00, '2026-03-12', 'EMEA', 'Direct');
