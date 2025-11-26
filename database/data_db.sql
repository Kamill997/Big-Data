CREATE DATABASE IF NOT EXISTS data_db;
USE data_db;

CREATE TABLE IF NOT EXISTS user_interest (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_email VARCHAR(255),
        airport_code VARCHAR(10),
        UNIQUE(user_email, airport_code)
);

CREATE TABLE IF NOT EXISTS flights (
    id INT AUTO_INCREMENT PRIMARY KEY,
    icao_flight VARCHAR(20) NOT NULL,
    icao_airport VARCHAR(10) NOT NULL,
    origin_country VARCHAR(50) NOT NULL,
    departure_time BIGINT,
    arrival_time BIGINT,
    is_arrival BOOLEAN
);
