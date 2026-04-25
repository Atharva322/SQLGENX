-- Large, repeatable seed for demo/evaluation workloads.
-- This script intentionally resets core tables and repopulates with synthetic data.

BEGIN;

TRUNCATE TABLE sales, employees, departments RESTART IDENTITY CASCADE;

INSERT INTO departments (name, cost_center)
VALUES
    ('Engineering', 'CC-100'),
    ('Sales', 'CC-200'),
    ('Finance', 'CC-300'),
    ('Marketing', 'CC-400'),
    ('Operations', 'CC-500'),
    ('People', 'CC-600'),
    ('Support', 'CC-700'),
    ('Legal', 'CC-800'),
    ('IT', 'CC-900'),
    ('Data', 'CC-1000'),
    ('Product', 'CC-1100'),
    ('Security', 'CC-1200');

INSERT INTO employees (department_id, first_name, last_name, title, hired_at, salary)
SELECT
    1 + ((gs - 1) % (SELECT COUNT(*) FROM departments)) AS department_id,
    'First' || gs::text AS first_name,
    'Last' || gs::text AS last_name,
    CASE (gs % 8)
        WHEN 0 THEN 'Analyst'
        WHEN 1 THEN 'Specialist'
        WHEN 2 THEN 'Engineer'
        WHEN 3 THEN 'Manager'
        WHEN 4 THEN 'Director'
        WHEN 5 THEN 'Coordinator'
        WHEN 6 THEN 'Associate'
        ELSE 'Lead'
    END AS title,
    DATE '2019-01-01' + ((random() * 2300)::int) AS hired_at,
    ROUND((55000 + (random() * 145000))::numeric, 2) AS salary
FROM generate_series(1, 900) AS gs;

INSERT INTO sales (employee_id, amount, sale_date, region, channel)
SELECT
    (1 + floor(random() * 900))::int AS employee_id,
    ROUND((250 + random() * 60000)::numeric, 2) AS amount,
    DATE '2024-01-01' + ((random() * 850)::int) AS sale_date,
    (ARRAY['North America', 'EMEA', 'APAC', 'LATAM', 'ANZ', 'Middle East'])[
        1 + floor(random() * 6)::int
    ] AS region,
    (ARRAY['Direct', 'Partner', 'Online', 'Field'])[
        1 + floor(random() * 4)::int
    ] AS channel
FROM generate_series(1, 25000);

COMMIT;
