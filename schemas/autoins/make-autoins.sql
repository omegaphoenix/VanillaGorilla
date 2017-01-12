-- This table holds individual car details.
CREATE TABLE cars (
    car_id          INTEGER     PRIMARY KEY,

    vin             CHAR(17)    NOT NULL UNIQUE,
    license_plate   VARCHAR(10) NOT NULL UNIQUE,
    license_state   CHAR(2)     NOT NULL,

    make            VARCHAR(20) NOT NULL,
    model           VARCHAR(20) NOT NULL,
    year            INTEGER     NOT NULL
);

-- This table holds details on customer insurance policies.
CREATE TABLE policies (
    policy_id       INTEGER     PRIMARY KEY,
    last_name       VARCHAR(50) NOT NULL,
    first_name      VARCHAR(50) NOT NULL,

    customer_street VARCHAR(200),
    customer_city   VARCHAR(20),
    customer_state  CHAR(2),
    customer_zip    CHAR(5)
);

-- This table associates cars with policies.  Note that a car can be covered
-- by multiple policies, and policy can cover multiple cars.
CREATE TABLE covered_by (
    car_id          INTEGER     REFERENCES cars,
    policy_id       INTEGER     REFERENCES policies,

    PRIMARY KEY (car_id, policy_id)
);

-- Details of specific insurance claims.
CREATE TABLE claims (
    claim_id        INTEGER     PRIMARY KEY,
    policy_id       INTEGER     NOT NULL REFERENCES policies,
    car_id          INTEGER     NOT NULL REFERENCES cars,

    claim_description   VARCHAR(2000),
    claim_amount    INTEGER
);


INSERT INTO cars (car_id, vin, license_plate, license_state, make, model, year)
VALUES (1011, '1A1234F5678G9123H', 'ABC123', 'CA', 'Toyota', 'Corolla', 2002);

INSERT INTO cars (car_id, vin, license_plate, license_state, make, model, year)
VALUES (1024, '1C2345P6789R1234S', '1XYZ987', 'AK', 'Nissan', 'Altima', 2005);

INSERT INTO cars (car_id, vin, license_plate, license_state, make, model, year)
VALUES (1039, '1E9886T4314U2241E', 'EDB405', 'NY', 'Porsche', 'Boxter', 2006);

INSERT INTO cars (car_id, vin, license_plate, license_state, make, model, year)
VALUES (1056, '2A7889E2341R9090W', '7UVW342', 'TX', 'GMC', 'Sierra', 2004);


INSERT INTO policies (policy_id, last_name, first_name,
    customer_street, customer_city, customer_state, customer_zip)
VALUES (80101330, 'Smith', 'John', '123 Evergreen Lane', 'Albany', 'NY', '11111');

INSERT INTO policies (policy_id, last_name, first_name,
    customer_street, customer_city, customer_state, customer_zip)
VALUES (80101337, 'Smith', 'Jane', '123 Evergreen Lane', 'Albany', 'NY', '11111');

INSERT INTO policies (policy_id, last_name, first_name,
    customer_street, customer_city, customer_state, customer_zip)
VALUES (80101520, 'Jones', 'Bob', '35 Broad St.', 'Springfield', 'AK', '11111');


INSERT INTO covered_by (car_id, policy_id) VALUES (1039, 80101330);
INSERT INTO covered_by (car_id, policy_id) VALUES (1039, 80101337);
INSERT INTO covered_by (car_id, policy_id) VALUES (1024, 80101520);

