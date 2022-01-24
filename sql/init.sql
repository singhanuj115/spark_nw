CREATE TABLE USERS(
       id INT,
       createdAt TIMESTAMP,
       age INT,
       city CHAR(150),
       country CHAR(150),
       emailDomain CHAR(150),
       gender CHAR(150),
       smoking CHAR(150),
       income INT,
       updatedAt TIMESTAMP
    );


CREATE TABLE SUBSCRIPTION(
       id INT,
       createdAt TIMESTAMP,
       updatedAt TIMESTAMP,
       status CHAR(150)
    );

CREATE TABLE MESSAGE(
       createdAt TIMESTAMP,
       receiverId INT,
       senderId INT
    );

