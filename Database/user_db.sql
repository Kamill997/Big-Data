CREATE DATABASE IF NOT EXISTS user_db;
USE user_db;

CREATE TABLE IF NOT EXISTS users (
    email VARCHAR(255) PRIMARY KEY,
    name VARCHAR(100),
    surname VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS requestId (
    id VARCHAR(100),
    esito_richiesta VARCHAR(100)
    );