CREATE TABLE IF NOT EXISTS gold.fct_trips (
    trip_key BIGINT,
    pickup_date_key INT,
    pickup_date DATE,
    pu_zone_key INT,
    do_zone_key INT,
    service_type TEXT,
    payment_type TEXT,
    vendor_id INT,
    passenger_count INT,
    trip_distance NUMERIC,
    total_amount NUMERIC
)
PARTITION BY RANGE (pickup_date);

CREATE TABLE gold.fct_trips_2022_01
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');

CREATE TABLE gold.fct_trips_2022_02
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');

CREATE TABLE gold.fct_trips_2022_03
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');

CREATE TABLE gold.fct_trips_2022_04
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');

CREATE TABLE gold.fct_trips_2022_05
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-05-01') TO ('2022-06-01');

CREATE TABLE gold.fct_trips_2022_06
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-06-01') TO ('2022-07-01');

CREATE TABLE gold.fct_trips_2022_07
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-07-01') TO ('2022-08-01');

CREATE TABLE gold.fct_trips_2022_08
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-08-01') TO ('2022-09-01');

CREATE TABLE gold.fct_trips_2022_09
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-09-01') TO ('2022-10-01');

CREATE TABLE gold.fct_trips_2022_10
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-10-01') TO ('2022-11-01');

CREATE TABLE gold.fct_trips_2022_11
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-11-01') TO ('2022-12-01');

CREATE TABLE gold.fct_trips_2022_12
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2022-12-01') TO ('2023-01-01');

CREATE TABLE gold.fct_trips_2023_01
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE gold.fct_trips_2023_02
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');

CREATE TABLE gold.fct_trips_2023_03
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');

CREATE TABLE gold.fct_trips_2023_04
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-04-01') TO ('2023-05-01');

CREATE TABLE gold.fct_trips_2023_05
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-05-01') TO ('2023-06-01');

CREATE TABLE gold.fct_trips_2023_06
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-06-01') TO ('2023-07-01');

CREATE TABLE gold.fct_trips_2023_07
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-07-01') TO ('2023-08-01');

CREATE TABLE gold.fct_trips_2023_08
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-08-01') TO ('2023-09-01');

CREATE TABLE gold.fct_trips_2023_09
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');

CREATE TABLE gold.fct_trips_2023_10
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');

CREATE TABLE gold.fct_trips_2023_11
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');

CREATE TABLE gold.fct_trips_2023_12
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');

CREATE TABLE gold.fct_trips_2024_01
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE gold.fct_trips_2024_02
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE gold.fct_trips_2024_03
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

CREATE TABLE gold.fct_trips_2024_04
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

CREATE TABLE gold.fct_trips_2024_05
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');

CREATE TABLE gold.fct_trips_2024_06
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');

CREATE TABLE gold.fct_trips_2024_07
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');

CREATE TABLE gold.fct_trips_2024_08
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

CREATE TABLE gold.fct_trips_2024_09
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE gold.fct_trips_2024_10
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE gold.fct_trips_2024_11
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE gold.fct_trips_2024_12
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE gold.fct_trips_2025_01
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE gold.fct_trips_2025_02
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE gold.fct_trips_2025_03
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE gold.fct_trips_2025_04
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

CREATE TABLE gold.fct_trips_2025_05
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

CREATE TABLE gold.fct_trips_2025_06
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

CREATE TABLE gold.fct_trips_2025_07
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE gold.fct_trips_2025_08
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

CREATE TABLE gold.fct_trips_2025_09
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE gold.fct_trips_2025_10
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE gold.fct_trips_2025_11
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE gold.fct_trips_2025_12
PARTITION OF gold.fct_trips
FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- Particiones por HASH

CREATE TABLE IF NOT EXISTS gold.dim_zone (
    zone_key INT PRIMARY KEY,
    borough TEXT,
    zone TEXT,
    service_zone TEXT
)
PARTITION BY HASH (zone_key);

CREATE TABLE gold.dim_zone_p0
PARTITION OF gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE gold.dim_zone_p1
PARTITION OF gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE gold.dim_zone_p2
PARTITION OF gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE gold.dim_zone_p3
PARTITION OF gold.dim_zone
FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- Particiones por LIST

CREATE TABLE IF NOT EXISTS gold.dim_service_type (
    service_type TEXT PRIMARY KEY
)
PARTITION BY LIST (service_type);

CREATE TABLE IF NOT EXISTS gold.dim_service_type_yellow
PARTITION OF gold.dim_service_type
FOR VALUES IN ('yellow');

CREATE TABLE IF NOT EXISTS gold.dim_service_type_green
PARTITION OF gold.dim_service_type
FOR VALUES IN ('green');

CREATE TABLE IF NOT EXISTS gold.dim_payment_type (
    payment_type_key INT PRIMARY KEY,
    payment_type TEXT
)
PARTITION BY LIST (payment_type_key);

CREATE TABLE gold.dim_payment_type_card
PARTITION OF gold.dim_payment_type
FOR VALUES IN (1);

CREATE TABLE gold.dim_payment_type_cash
PARTITION OF gold.dim_payment_type
FOR VALUES IN (2);

CREATE TABLE gold.dim_payment_type_other
PARTITION OF gold.dim_payment_type
FOR VALUES IN (0,3,4,5);

