CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CREATE SCHEMA FOR FINAL AREA
CREATE SCHEMA IF NOT EXISTS final AUTHORIZATION postgres;

--------------------------------------------------------------------------------------------------------------------------------- FINAL SCHEMA
-- time dimension
DROP TABLE if exists final.dim_time;
CREATE TABLE final.dim_time
(
	time_id integer NOT NULL,
	time_actual time NOT NULL,
	hours_24 character(2) NOT NULL,
	hours_12 character(2) NOT NULL,
	hour_minutes character (2)  NOT NULL,
	day_minutes integer NOT NULL,
	day_time_name character varying (20) NOT NULL,
	day_night character varying (20) NOT NULL,
	CONSTRAINT time_pk PRIMARY KEY (time_id)
);

DROP TABLE if exists final.dim_date;
CREATE TABLE final.dim_date
(
  date_id              INT NOT null primary KEY,
  date_actual              DATE NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             VARCHAR(20) NOT NULL
);

CREATE INDEX dim_date_date_actual_idx
  ON final.dim_date(date_actual);


-- dim passenger
CREATE TABLE final.dim_passenger (
    passenger_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    passenger_nk VARCHAR(20) NOT NULL,
    passenger_name VARCHAR(255),
    phone VARCHAR(30),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE final.dim_aircraft (
    aircraft_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_nk BPCHAR(3) NOT NULL,
    model VARCHAR(255),
    "range" INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_airport
CREATE TABLE final.dim_airport (
    airport_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    airport_nk BPCHAR(3) NOT NULL,
    airport_name VARCHAR(255),
    city VARCHAR(255),
    coordinates POINT,
    timezone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE final.dim_seat (
    seat_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID,
    seat_no VARCHAR(4) NOT NULL,
    fare_conditions VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_aircraft_seat FOREIGN KEY (aircraft_id) REFERENCES final.dim_aircraft(aircraft_id)
);

CREATE TABLE final.fct_booking_ticket (
    booking_ticket_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    book_nk BPCHAR(6),
    ticket_no BPCHAR(13),
    passenger_id UUID,
    flight_nk INT4,
    flight_no VARCHAR,
    -- Foreign Keys
    book_date_local INT,
    book_date_utc INT,
    book_time_local INT,
    book_time_utc INT,
    scheduled_departure_date_local INT,
    scheduled_departure_date_utc INT,
    scheduled_departure_time_local INT,
    scheduled_departure_time_utc INT,
    scheduled_arrival_date_local INT,
    scheduled_arrival_date_utc INT,
    scheduled_arrival_time_local INT,
    scheduled_arrival_time_utc INT,
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    actual_departure_date_local INT,
    actual_departure_date_utc INT,
    actual_departure_time_local INT,
    actual_departure_time_utc INT,
    actual_arrival_date_local INT,
    actual_arrival_date_utc INT,
    actual_arrival_time_local INT,
    actual_arrival_time_utc INT,
    fare_conditions VARCHAR(10),
    amount NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_booking_passenger_id FOREIGN KEY (passenger_id) REFERENCES final.dim_passenger(passenger_id),
    CONSTRAINT fk_book_date_local FOREIGN KEY (book_date_local) REFERENCES final.dim_date,
    CONSTRAINT fk_book_date_utc FOREIGN KEY (book_date_utc) REFERENCES final.dim_date,
    CONSTRAINT fk_book_time_local FOREIGN KEY (book_time_local) REFERENCES final.dim_time,
    CONSTRAINT fk_book_time_utc FOREIGN KEY (book_time_utc) REFERENCES final.dim_time,
    CONSTRAINT fk_scheduled_departure_date_local FOREIGN KEY (scheduled_departure_date_local) REFERENCES final.dim_date,
    CONSTRAINT fk_scheduled_departure_date_utc FOREIGN KEY (scheduled_departure_date_utc) REFERENCES final.dim_date,
    CONSTRAINT fk_scheduled_departure_time_local FOREIGN KEY (scheduled_departure_time_local) REFERENCES final.dim_time,
    CONSTRAINT fk_scheduled_departure_time_utc FOREIGN KEY (scheduled_departure_time_utc) REFERENCES final.dim_time,
    CONSTRAINT fk_scheduled_arrival_date_local FOREIGN KEY (scheduled_arrival_date_local) REFERENCES final.dim_date,
    CONSTRAINT fk_scheduled_arrival_date_utc FOREIGN KEY (scheduled_arrival_date_utc) REFERENCES final.dim_date,
    CONSTRAINT fk_scheduled_arrival_time_local FOREIGN KEY (scheduled_arrival_time_local) REFERENCES final.dim_time,
    CONSTRAINT fk_scheduled_arrival_time_utc FOREIGN KEY (scheduled_arrival_time_utc) REFERENCES final.dim_time,
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES final.dim_aircraft(aircraft_id),
    CONSTRAINT fk_actual_departure_date_local FOREIGN KEY (actual_departure_date_local) REFERENCES final.dim_date,
    CONSTRAINT fk_actual_departure_date_utc FOREIGN KEY (actual_departure_date_utc) REFERENCES final.dim_date,
    CONSTRAINT fk_actual_departure_time_local FOREIGN KEY (actual_departure_time_local) REFERENCES final.dim_time,
    CONSTRAINT fk_actual_departure_time_utc FOREIGN KEY (actual_departure_time_utc) REFERENCES final.dim_time,
    CONSTRAINT fk_actual_arrival_date_local FOREIGN KEY (actual_arrival_date_local) REFERENCES final.dim_date,
    CONSTRAINT fk_actual_arrival_date_utc FOREIGN KEY (actual_arrival_date_utc) REFERENCES final.dim_date,
    CONSTRAINT fk_actual_arrival_time_local FOREIGN KEY (actual_arrival_time_local) REFERENCES final.dim_time,
    CONSTRAINT fk_actual_arrival_time_utc FOREIGN KEY (actual_arrival_time_utc) REFERENCES final.dim_time
);


CREATE TABLE final.fct_flight_activity (
    flight_activity_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    flight_nk BPCHAR(6),
    flight_no VARCHAR,
    -- Foreign Keys
    scheduled_departure_date_local INT,
    scheduled_departure_date_utc INT,
    scheduled_departure_time_local INT,
    scheduled_departure_time_utc INT,
    scheduled_arrival_date_local INT,
    scheduled_arrival_date_utc INT,
    scheduled_arrival_time_local INT,
    scheduled_arrival_time_utc INT,
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    actual_departure_date_local INT,
    actual_departure_date_utc INT,
    actual_departure_time_local INT,
    actual_departure_time_utc INT,
    actual_arrival_date_local INT,
    actual_arrival_date_utc INT,
    actual_arrival_time_local INT,
    actual_arrival_time_utc INT,
    status VARCHAR(20),
    delay_departure INTERVAL,
    delay_arrival INTERVAL,
    travel_time INTERVAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_scheduled_departure_date_local FOREIGN KEY (scheduled_departure_date_local) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_date_utc FOREIGN KEY (scheduled_departure_date_utc) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_time_local FOREIGN KEY (scheduled_departure_time_local) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_scheduled_departure_time_utc FOREIGN KEY (scheduled_departure_time_utc) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_date_local FOREIGN KEY (scheduled_arrival_date_local) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_date_utc FOREIGN KEY (scheduled_arrival_date_utc) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_time_local FOREIGN KEY (scheduled_arrival_time_local) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_time_utc FOREIGN KEY (scheduled_arrival_time_utc) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES final.dim_aircraft(aircraft_id),
    CONSTRAINT fk_actual_departure_date_local FOREIGN KEY (actual_departure_date_local) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_actual_departure_date_utc FOREIGN KEY (actual_departure_date_utc) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_actual_departure_time_local FOREIGN KEY (actual_departure_time_local) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_actual_departure_time_utc FOREIGN KEY (actual_departure_time_utc) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_actual_arrival_date_local FOREIGN KEY (actual_arrival_date_local) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_actual_arrival_date_utc FOREIGN KEY (actual_arrival_date_utc) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_actual_arrival_time_local FOREIGN KEY (actual_arrival_time_local) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_actual_arrival_time_utc FOREIGN KEY (actual_arrival_time_utc) REFERENCES final.dim_time(time_id)
);

CREATE TABLE final.fct_seat_occupied_daily (
    seat_occupied_daily_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date_flight INT,
    flight_nk BPCHAR(6),
    flight_no VARCHAR,
    -- Foreign Keys
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    status VARCHAR(20),
    total_seat NUMERIC,
    seat_occupied NUMERIC,
    empty_seats NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_date_flight FOREIGN KEY (date_flight) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES final.dim_aircraft(aircraft_id)
);

CREATE TABLE final.fct_boarding_pass (
    boarding_pass_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticket_no BPCHAR(13),
    book_ref BPCHAR(6),
    passenger_id UUID,
    flight_id INT,
    flight_no VARCHAR,
    boarding_no INT,
    -- Foreign Keys
    scheduled_departure_date_local INT,
    scheduled_departure_date_utc INT,
    scheduled_departure_time_local INT,
    scheduled_departure_time_utc INT,
    scheduled_arrival_date_local INT,
    scheduled_arrival_date_utc INT,
    scheduled_arrival_time_local INT,
    scheduled_arrival_time_utc INT,
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    status VARCHAR(20),
    fare_conditions VARCHAR(10),
    seat_no VARCHAR(4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_passenger_id FOREIGN KEY (passenger_id) REFERENCES final.dim_passenger(passenger_id),
    CONSTRAINT fk_scheduled_departure_date_local FOREIGN KEY (scheduled_departure_date_local) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_date_utc FOREIGN KEY (scheduled_departure_date_utc) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_time_local FOREIGN KEY (scheduled_departure_time_local) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_scheduled_departure_time_utc FOREIGN KEY (scheduled_departure_time_utc) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_date_local FOREIGN KEY (scheduled_arrival_date_local) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_date_utc FOREIGN KEY (scheduled_arrival_date_utc) REFERENCES final.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_time_local FOREIGN KEY (scheduled_arrival_time_local) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_time_utc FOREIGN KEY (scheduled_arrival_time_utc) REFERENCES final.dim_time(time_id),
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES final.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES final.dim_aircraft(aircraft_id)
);

INSERT INTO final.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

-- populate time dimension
insert into  final.dim_time

SELECT  
	cast(to_char(minute, 'hh24mi') as numeric) time_id,
	to_char(minute, 'hh24:mi')::time AS tume_actual,
	-- Hour of the day (0 - 23)
	to_char(minute, 'hh24') AS hour_24,
	-- Hour of the day (0 - 11)
	to_char(minute, 'hh12') hour_12,
	-- Hour minute (0 - 59)
	to_char(minute, 'mi') hour_minutes,
	-- Minute of the day (0 - 1439)
	extract(hour FROM minute)*60 + extract(minute FROM minute) day_minutes,
	-- Names of day periods
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '00:00' AND '11:59'
		then 'AM'
		when to_char(minute, 'hh24:mi') BETWEEN '12:00' AND '23:59'
		then 'PM'
	end AS day_time_name,
	-- Indicator of day or night
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '07:00' AND '19:59' then 'Day'	
		else 'Night'
	end AS day_night
FROM 
	(SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute 
	FROM  generate_series(0,1439) AS sequence(minute)
GROUP BY sequence.minute
) DQ
ORDER BY 1;


-- Add Unique Constraints to fact tables
ALTER TABLE final.fct_booking_ticket
ADD CONSTRAINT fct_booking_ticket_unique UNIQUE (book_nk, ticket_no, flight_nk);

ALTER TABLE final.fct_flight_activity
ADD CONSTRAINT fct_flight_activity_unique UNIQUE (flight_nk);

ALTER TABLE final.fct_seat_occupied_daily
ADD CONSTRAINT fct_seat_occupied_daily_unique UNIQUE (date_flight, flight_nk);

ALTER TABLE final.fct_boarding_pass
ADD CONSTRAINT fct_boarding_pass_unique UNIQUE (ticket_no, flight_id, boarding_no);