CREATE TABLE IF NOT EXISTS departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    cost_center VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    department_id INTEGER REFERENCES departments(id),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    title VARCHAR(120),
    hired_at DATE NOT NULL,
    salary NUMERIC(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id),
    amount NUMERIC(12,2) NOT NULL,
    sale_date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    channel VARCHAR(50) NOT NULL
);

INSERT INTO departments (name, cost_center)
VALUES
    ('Engineering', 'CC-100'),
    ('Sales', 'CC-200'),
    ('Finance', 'CC-300')
ON CONFLICT (name) DO NOTHING;

INSERT INTO employees (department_id, first_name, last_name, title, hired_at, salary)
SELECT d.id, v.first_name, v.last_name, v.title, v.hired_at, v.salary
FROM (
    VALUES
        ('Engineering', 'Ava', 'Shaw', 'Software Engineer', '2022-03-01', 125000.00),
        ('Sales', 'Liam', 'Patel', 'Account Executive', '2021-08-15', 98000.00),
        ('Finance', 'Noah', 'Kim', 'Financial Analyst', '2020-01-10', 110000.00)
) AS v(dept_name, first_name, last_name, title, hired_at, salary)
JOIN departments d ON d.name = v.dept_name
WHERE NOT EXISTS (
    SELECT 1
    FROM employees e
    WHERE e.first_name = v.first_name
      AND e.last_name = v.last_name
);

INSERT INTO sales (employee_id, amount, sale_date, region, channel)
SELECT e.id, v.amount, v.sale_date, v.region, v.channel
FROM (
    VALUES
        ('Liam', 'Patel', 25000.00, '2026-01-15', 'North America', 'Direct'),
        ('Liam', 'Patel', 18000.00, '2026-02-20', 'North America', 'Partner'),
        ('Liam', 'Patel', 32000.00, '2026-03-12', 'EMEA', 'Direct')
) AS v(first_name, last_name, amount, sale_date, region, channel)
JOIN employees e ON e.first_name = v.first_name AND e.last_name = v.last_name
WHERE NOT EXISTS (
    SELECT 1
    FROM sales s
    WHERE s.employee_id = e.id
      AND s.amount = v.amount
      AND s.sale_date = v.sale_date
);
