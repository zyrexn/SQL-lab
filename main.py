# CodeGrade step0
# Run this cell without changes

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)
# CodeGrade step1
# Query employees in Boston by joining employees and offices tables
# Filter results where the office city is 'Boston'
df_boston = pd.read_sql("""
    SELECT e.firstName, e.lastName, e.jobTitle
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
""", conn)

df_boston
# CodeGrade step2
# Find offices with zero employees using a LEFT JOIN
# Offices with no matching employees will have NULL in the employee fields
df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city
    FROM offices o
    LEFT JOIN employees e ON o.officeCode = e.officeCode
    WHERE e.employeeNumber IS NULL
""", conn)

df_zero_emp
# CodeGrade step3
# Return ALL employees with their office city and state using a LEFT JOIN
# LEFT JOIN ensures employees without an office are still included
# Order by first name, then last name alphabetically
df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)

df_employee
# CodeGrade step4
# Return contact info for customers who have NOT placed any orders
# LEFT JOIN customers to orders, then filter where no order exists (NULL)
# Sort alphabetically by contact last name
df_contacts = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName
""", conn)

df_contacts
# CodeGrade step5
# Return customer contact names with their payment amounts and dates
# JOIN customers to payments using the shared customerNumber column
# CAST amount to REAL to ensure correct numeric sorting (not alphabetical)
# Sort by payment amount in descending order
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    JOIN payments p ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC
""", conn)

df_payment
# CodeGrade step6
# Return employees whose customers have an average credit limit over $90,000
# JOIN employees to customers using salesRepEmployeeNumber as the foreign key
# GROUP BY employee, use HAVING to filter groups where avg credit limit exceeds 90k
# Sort by number of customers from highest to lowest
df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS numCustomers
    FROM employees e
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber, e.firstName, e.lastName
    HAVING AVG(CAST(c.creditLimit AS REAL)) > 90000
    ORDER BY numCustomers DESC
""", conn)

df_credit
# CodeGrade step7
# Return each product's name, number of orders, and total quantity sold
# JOIN products to orderdetails using productCode
# COUNT orders as 'numorders', SUM quantityOrdered as 'totalunits'
# GROUP BY product and sort by totalunits highest to lowest
df_product_sold = pd.read_sql("""
    SELECT p.productName,
           COUNT(od.orderNumber) AS numorders,
           SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    GROUP BY p.productCode, p.productName
    ORDER BY totalunits DESC
""", conn)

df_product_sold
# CodeGrade step8
# Return each product's name, code, and number of distinct customers who ordered it
# Chain joins: products -> orderdetails -> orders -> customers
# Use COUNT(DISTINCT) to avoid counting the same customer multiple times per product
# Sort by highest number of purchasers
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode,
           COUNT(DISTINCT c.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    JOIN customers c ON o.customerNumber = c.customerNumber
    GROUP BY p.productCode, p.productName
    ORDER BY numpurchasers DESC
""", conn)

df_total_customers
# CodeGrade step9
# Return the number of customers per office to assess staffing levels
# Chain joins: offices -> employees -> customers using officeCode and salesRepEmployeeNumber
# COUNT distinct customers per office, return office code and city
# Sort by highest number of customers
df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city,
           COUNT(DISTINCT c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e ON o.officeCode = e.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode, o.city
    ORDER BY n_customers DESC
""", conn)

df_customers
# CodeGrade step10
# Return employees who sold products ordered by fewer than 20 distinct customers
# Subquery: find all productCodes with fewer than 20 distinct purchasers (adapted from step 8)
# Outer query: trace the sales pipeline to find which employees sold those products
# Chain joins: employees -> orders -> orderdetails, filtered by the subquery
# Use DISTINCT to avoid duplicate employee rows, also return their office city and code
df_under_20 = pd.read_sql("""
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders ord ON c.customerNumber = ord.customerNumber
    JOIN orderdetails od ON ord.orderNumber = od.orderNumber
    WHERE od.productCode IN (
        -- Subquery: products ordered by fewer than 20 distinct customers
        SELECT p.productCode
        FROM products p
        JOIN orderdetails od2 ON p.productCode = od2.productCode
        JOIN orders o2 ON od2.orderNumber = o2.orderNumber
        JOIN customers c2 ON o2.customerNumber = c2.customerNumber
        GROUP BY p.productCode
        HAVING COUNT(DISTINCT c2.customerNumber) < 20
    )
""", conn)

df_under_20
# Run this cell without changes

conn.close()