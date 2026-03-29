-- 1. Create the database
CREATE DATABASE IF NOT EXISTS autodba_test;
USE autodba_test;

-- 2. Create the Users table
CREATE TABLE Users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    registration_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 3. Create the Products table
CREATE TABLE Products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2)
);

-- 4. Create the Orders table (The intentionally un-optimized table)
CREATE TABLE Orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    product_id INT,
    order_date DATETIME,
    status VARCHAR(20),
    -- We are linking the tables, but intentionally NOT creating indexes 
    -- on user_id or product_id here. This is what will cause the lag later!
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

-- Create a dedicated user for your proxy and AI scripts
CREATE USER 'autodba_admin'@'localhost' IDENTIFIED BY 'StrongPassword123!';

-- Give this user full control, but ONLY over the autodba_test database
GRANT ALL PRIVILEGES ON autodba_test.* TO 'autodba_admin'@'localhost';

-- Apply the changes
FLUSH PRIVILEGES;