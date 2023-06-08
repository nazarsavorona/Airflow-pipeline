-- 1 / Write SQL query to get all employees with the 5th highest salary (keep in mind there
-- could be more than 1 person with the same salary, we have to get all employees).

-- Employee’s table structure:
-- - employee_id INT
-- - department_id INT
-- - hire_date DATE
-- - salary DOUBLE

SELECT *
FROM employee
WHERE salary = (
    SELECT DISTINCT salary
    FROM employee
    ORDER BY salary DESC
    LIMIT 1 OFFSET 4
);

-- 2 / You have two tables: country, city

-- Country’s table structure:
-- - country_id,
-- - country_name

-- City’s table structure:
-- - city_id,
-- - city_name,
-- - country_id

-- Write SQL query to find countries which do not have cities.

SELECT country.country_name
FROM country LEFT JOIN city ON country.country_id = city.country_id
WHERE city.city_id IS NULL;
