CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CREATE SCHEMA FOR STAGING
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION postgres;
------------------------------------------------------------------------------------------------------------------------------ STAGING SCHEMA
COMMENT ON SCHEMA stg IS 'Airlines demo database schema';

-- DROP SEQUENCE stg.flights_flight_id_seq;

CREATE SEQUENCE stg.flights_flight_id_seq
	MINVALUE 0
	NO MAXVALUE
	START 0
	NO CYCLE;

-- Permissions

ALTER SEQUENCE stg.flights_flight_id_seq OWNER TO postgres;
GRANT ALL ON SEQUENCE stg.flights_flight_id_seq TO postgres;
-- stg.aircrafts_data definition

-- Drop table

-- DROP TABLE stg.aircrafts_data;

CREATE TABLE stg.aircrafts_data (
    id uuid default uuid_generate_v4(),
	aircraft_code bpchar(3) NOT NULL, -- Aircraft code, IATA
	model jsonb NOT NULL, -- Aircraft model
	"range" int4 NOT NULL, -- Maximal flying distance, km
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT aircrafts_pkey PRIMARY KEY (aircraft_code),
	CONSTRAINT aircrafts_range_check CHECK ((range > 0))
);
COMMENT ON TABLE stg.aircrafts_data IS 'Aircrafts (internal data)';

-- Column comments

COMMENT ON COLUMN stg.aircrafts_data.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.aircrafts_data.model IS 'Aircraft model';
COMMENT ON COLUMN stg.aircrafts_data."range" IS 'Maximal flying distance, km';

-- Permissions

ALTER TABLE stg.aircrafts_data OWNER TO postgres;
GRANT ALL ON TABLE stg.aircrafts_data TO postgres;


-- stg.airports_data definition

-- Drop table

-- DROP TABLE stg.airports_data;

CREATE TABLE stg.airports_data (
    id uuid default uuid_generate_v4(),
	airport_code bpchar(3) NOT NULL, -- Airport code
	airport_name jsonb NOT NULL, -- Airport name
	city jsonb NOT NULL, -- City
	coordinates point NOT NULL, -- Airport coordinates (longitude and latitude)
	timezone text NOT NULL, -- Airport time zone
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT airports_data_pkey PRIMARY KEY (airport_code)
);
COMMENT ON TABLE stg.airports_data IS 'Airports (internal data)';

-- Column comments

COMMENT ON COLUMN stg.airports_data.airport_code IS 'Airport code';
COMMENT ON COLUMN stg.airports_data.airport_name IS 'Airport name';
COMMENT ON COLUMN stg.airports_data.city IS 'City';
COMMENT ON COLUMN stg.airports_data.coordinates IS 'Airport coordinates (longitude and latitude)';
COMMENT ON COLUMN stg.airports_data.timezone IS 'Airport time zone';

-- Permissions

ALTER TABLE stg.airports_data OWNER TO postgres;
GRANT ALL ON TABLE stg.airports_data TO postgres;


-- stg.bookings definition

-- Drop table

-- DROP TABLE stg.bookings;

CREATE TABLE stg.bookings (
    id uuid default uuid_generate_v4(),
	book_ref bpchar(6) NOT NULL, -- Booking number
	book_date timestamptz NOT NULL, -- Booking date
	total_amount numeric(10, 2) NOT NULL, -- Total booking cost
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT bookings_pkey PRIMARY KEY (book_ref)
);
COMMENT ON TABLE stg.bookings IS 'Bookings';

-- Column comments

COMMENT ON COLUMN stg.bookings.book_ref IS 'Booking number';
COMMENT ON COLUMN stg.bookings.book_date IS 'Booking date';
COMMENT ON COLUMN stg.bookings.total_amount IS 'Total booking cost';

-- Permissions

ALTER TABLE stg.bookings OWNER TO postgres;
GRANT ALL ON TABLE stg.bookings TO postgres;


-- stg.flights definition

-- Drop table

-- DROP TABLE stg.flights;

CREATE TABLE stg.flights (
    id uuid default uuid_generate_v4(),
	flight_id serial4 NOT NULL, -- Flight ID
	flight_no bpchar(6) NOT NULL, -- Flight number
	scheduled_departure timestamptz NOT NULL, -- Scheduled departure time
	scheduled_arrival timestamptz NOT NULL, -- Scheduled arrival time
	departure_airport bpchar(3) NOT NULL, -- Airport of departure
	arrival_airport bpchar(3) NOT NULL, -- Airport of arrival
	status varchar(20) NOT NULL, -- Flight status
	aircraft_code bpchar(3) NOT NULL, -- Aircraft code, IATA
	actual_departure timestamptz NULL, -- Actual departure time
	actual_arrival timestamptz NULL, -- Actual arrival time
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT flights_check CHECK ((scheduled_arrival > scheduled_departure)),
	CONSTRAINT flights_check1 CHECK (((actual_arrival IS NULL) OR ((actual_departure IS NOT NULL) AND (actual_arrival IS NOT NULL) AND (actual_arrival > actual_departure)))),
	CONSTRAINT flights_flight_no_scheduled_departure_key UNIQUE (flight_no, scheduled_departure),
	CONSTRAINT flights_pkey PRIMARY KEY (flight_id),
	CONSTRAINT flights_status_check CHECK (((status)::text = ANY (ARRAY[('On Time'::character varying)::text, ('Delayed'::character varying)::text, ('Departed'::character varying)::text, ('Arrived'::character varying)::text, ('Scheduled'::character varying)::text, ('Cancelled'::character varying)::text]))),
	CONSTRAINT flights_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES stg.aircrafts_data(aircraft_code),
	CONSTRAINT flights_arrival_airport_fkey FOREIGN KEY (arrival_airport) REFERENCES stg.airports_data(airport_code),
	CONSTRAINT flights_departure_airport_fkey FOREIGN KEY (departure_airport) REFERENCES stg.airports_data(airport_code)
);
COMMENT ON TABLE stg.flights IS 'Flights';

-- Column comments

COMMENT ON COLUMN stg.flights.flight_id IS 'Flight ID';
COMMENT ON COLUMN stg.flights.flight_no IS 'Flight number';
COMMENT ON COLUMN stg.flights.scheduled_departure IS 'Scheduled departure time';
COMMENT ON COLUMN stg.flights.scheduled_arrival IS 'Scheduled arrival time';
COMMENT ON COLUMN stg.flights.departure_airport IS 'Airport of departure';
COMMENT ON COLUMN stg.flights.arrival_airport IS 'Airport of arrival';
COMMENT ON COLUMN stg.flights.status IS 'Flight status';
COMMENT ON COLUMN stg.flights.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.flights.actual_departure IS 'Actual departure time';
COMMENT ON COLUMN stg.flights.actual_arrival IS 'Actual arrival time';

-- Permissions

ALTER TABLE stg.flights OWNER TO postgres;
GRANT ALL ON TABLE stg.flights TO postgres;


-- stg.seats definition

-- Drop table

-- DROP TABLE stg.seats;

CREATE TABLE stg.seats (
    id uuid default uuid_generate_v4(),
	aircraft_code bpchar(3) NOT NULL, -- Aircraft code, IATA
	seat_no varchar(4) NOT NULL, -- Seat number
	fare_conditions varchar(10) NOT NULL, -- Travel class
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT seats_fare_conditions_check CHECK (((fare_conditions)::text = ANY (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text, ('Business'::character varying)::text]))),
	CONSTRAINT seats_pkey PRIMARY KEY (aircraft_code, seat_no),
	CONSTRAINT seats_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES stg.aircrafts_data(aircraft_code) ON DELETE CASCADE
);
COMMENT ON TABLE stg.seats IS 'Seats';

-- Column comments

COMMENT ON COLUMN stg.seats.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.seats.seat_no IS 'Seat number';
COMMENT ON COLUMN stg.seats.fare_conditions IS 'Travel class';

-- Permissions

ALTER TABLE stg.seats OWNER TO postgres;
GRANT ALL ON TABLE stg.seats TO postgres;


-- stg.tickets definition

-- Drop table

-- DROP TABLE stg.tickets;

CREATE TABLE stg.tickets (
    id uuid default uuid_generate_v4(),
	ticket_no bpchar(13) NOT NULL, -- Ticket number
	book_ref bpchar(6) NOT NULL, -- Booking number
	passenger_id varchar(20) NOT NULL, -- Passenger ID
	passenger_name text NOT NULL, -- Passenger name
	contact_data jsonb NULL, -- Passenger contact information
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT tickets_pkey PRIMARY KEY (ticket_no),
	CONSTRAINT tickets_book_ref_fkey FOREIGN KEY (book_ref) REFERENCES stg.bookings(book_ref)
);
COMMENT ON TABLE stg.tickets IS 'Tickets';

-- Column comments

COMMENT ON COLUMN stg.tickets.ticket_no IS 'Ticket number';
COMMENT ON COLUMN stg.tickets.book_ref IS 'Booking number';
COMMENT ON COLUMN stg.tickets.passenger_id IS 'Passenger ID';
COMMENT ON COLUMN stg.tickets.passenger_name IS 'Passenger name';
COMMENT ON COLUMN stg.tickets.contact_data IS 'Passenger contact information';

-- Permissions

ALTER TABLE stg.tickets OWNER TO postgres;
GRANT ALL ON TABLE stg.tickets TO postgres;


-- stg.ticket_flights definition

-- Drop table

-- DROP TABLE stg.ticket_flights;

CREATE TABLE stg.ticket_flights (
    id uuid default uuid_generate_v4(),
	ticket_no bpchar(13) NOT NULL, -- Ticket number
	flight_id int4 NOT NULL, -- Flight ID
	fare_conditions varchar(10) NOT NULL, -- Travel class
	amount numeric(10, 2) NOT NULL, -- Travel cost
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT ticket_flights_amount_check CHECK ((amount >= (0)::numeric)),
	CONSTRAINT ticket_flights_fare_conditions_check CHECK (((fare_conditions)::text = ANY (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text, ('Business'::character varying)::text]))),
	CONSTRAINT ticket_flights_pkey PRIMARY KEY (ticket_no, flight_id),
	CONSTRAINT ticket_flights_flight_id_fkey FOREIGN KEY (flight_id) REFERENCES stg.flights(flight_id),
	CONSTRAINT ticket_flights_ticket_no_fkey FOREIGN KEY (ticket_no) REFERENCES stg.tickets(ticket_no)
);
COMMENT ON TABLE stg.ticket_flights IS 'Flight segment';

-- Column comments

COMMENT ON COLUMN stg.ticket_flights.ticket_no IS 'Ticket number';
COMMENT ON COLUMN stg.ticket_flights.flight_id IS 'Flight ID';
COMMENT ON COLUMN stg.ticket_flights.fare_conditions IS 'Travel class';
COMMENT ON COLUMN stg.ticket_flights.amount IS 'Travel cost';

-- Permissions

ALTER TABLE stg.ticket_flights OWNER TO postgres;
GRANT ALL ON TABLE stg.ticket_flights TO postgres;


-- stg.boarding_passes definition

-- Drop table

-- DROP TABLE stg.boarding_passes;

CREATE TABLE stg.boarding_passes (
    id uuid default uuid_generate_v4(),
	ticket_no bpchar(13) NOT NULL, -- Ticket number
	flight_id int4 NOT NULL, -- Flight ID
	boarding_no int4 NOT NULL, -- Boarding pass number
	seat_no varchar(4) NOT NULL, -- Seat number
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT boarding_passes_flight_id_boarding_no_key UNIQUE (flight_id, boarding_no),
	CONSTRAINT boarding_passes_flight_id_seat_no_key UNIQUE (flight_id, seat_no),
	CONSTRAINT boarding_passes_pkey PRIMARY KEY (ticket_no, flight_id),
	CONSTRAINT boarding_passes_ticket_no_fkey FOREIGN KEY (ticket_no,flight_id) REFERENCES stg.ticket_flights(ticket_no,flight_id)
);
COMMENT ON TABLE stg.boarding_passes IS 'Boarding passes';

-- Column comments

COMMENT ON COLUMN stg.boarding_passes.ticket_no IS 'Ticket number';
COMMENT ON COLUMN stg.boarding_passes.flight_id IS 'Flight ID';
COMMENT ON COLUMN stg.boarding_passes.boarding_no IS 'Boarding pass number';
COMMENT ON COLUMN stg.boarding_passes.seat_no IS 'Seat number';

-- Permissions

ALTER TABLE stg.boarding_passes OWNER TO postgres;
GRANT ALL ON TABLE stg.boarding_passes TO postgres;