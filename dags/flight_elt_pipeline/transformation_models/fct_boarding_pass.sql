WITH 
    stg_tickets AS (
        SELECT * 
        FROM stg.tickets
    ),

    dim_passengers AS (
        SELECT *
        FROM final.dim_passenger 
    ),
    
    stg_ticket_flights AS (
        SELECT *
        FROM stg.ticket_flights
    ),
    
    stg_flights AS (
        SELECT *
        FROM stg.flights
    ),
    
    dim_dates AS (
        SELECT *
        FROM final.dim_date
    ),
    
    dim_times AS (
        SELECT *
        FROM final.dim_time
    ),
    
    dim_airports AS (
        SELECT *
        FROM final.dim_airport
    ),
    
    dim_aircrafts AS (
        SELECT *
        FROM final.dim_aircraft
    ),
    
    stg_boarding_passes AS (
        SELECT *
        FROM stg.boarding_passes
    ),
    
    final_fct_boarding_pass AS (
        SELECT 
            st.ticket_no AS ticket_no,
            st.book_ref AS book_ref,
            dp.passenger_id AS passenger_id,
            stf.flight_id AS flight_id,
            sf.flight_no AS flight_no,
            sbp.boarding_no AS boarding_no,
            dd1.date_id AS scheduled_departure_date_local,
            dd2.date_id AS scheduled_departure_date_utc,
            dt1.time_id AS scheduled_departure_time_local,
            dt2.time_id AS scheduled_departure_time_utc,
            dd3.date_id AS scheduled_arrival_date_local,
            dd4.date_id AS scheduled_arrival_date_utc,
            dt3.time_id AS scheduled_arrival_time_local,
            dt4.time_id AS scheduled_arrival_time_utc,
            da1.airport_id AS departure_airport,
            da2.airport_id AS arrival_airport,
            dac.aircraft_id AS aircraft_code,
            sf.status AS status,
            stf.fare_conditions AS fare_conditions,
            sbp.seat_no AS seat_no
            
        FROM stg_tickets st
        JOIN dim_passengers dp
            ON dp.passenger_id = st.id
        JOIN stg_ticket_flights stf 
            ON stf.ticket_no = st.ticket_no
        JOIN stg_flights sf 
            ON sf.flight_id = stf.flight_id
        JOIN dim_dates dd1 
            ON dd1.date_actual = DATE(sf.scheduled_departure)
        JOIN dim_dates dd2
            ON dd2.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
        JOIN dim_times dt1
            ON dt1.time_actual::time = (sf.scheduled_departure)::time
        JOIN dim_times dt2
            ON dt2.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
        JOIN dim_dates dd3
            ON dd3.date_actual = DATE(sf.scheduled_arrival)
        JOIN dim_dates dd4
            ON dd4.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
        JOIN dim_times dt3
            ON dt3.time_actual::time = (sf.scheduled_arrival)::time
        JOIN dim_times dt4
            ON dt4.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
        JOIN dim_airports da1 
            ON da1.airport_nk = sf.departure_airport
        JOIN dim_airports da2
            ON da2.airport_nk = sf.arrival_airport
        JOIN dim_aircrafts dac
            ON dac.aircraft_nk = sf.aircraft_code
        JOIN stg_boarding_passes sbp
            ON sbp.flight_id = stf.flight_id
            and sbp.ticket_no = stf.ticket_no
    )

INSERT INTO final.fct_boarding_pass (
    ticket_no, 
    book_ref, 
    passenger_id, 
    flight_id, 
    flight_no, 
    boarding_no, 
    scheduled_departure_date_local, 
    scheduled_departure_date_utc, 
    scheduled_departure_time_local, 
    scheduled_departure_time_utc, 
    scheduled_arrival_date_local, 
    scheduled_arrival_date_utc, 
    scheduled_arrival_time_local, 
    scheduled_arrival_time_utc, 
    departure_airport, 
    arrival_airport, 
    aircraft_code, 
    status, 
    fare_conditions, 
    seat_no
)

SELECT 
    * 
FROM 
    final_fct_boarding_pass

ON CONFLICT(ticket_no, flight_id, boarding_no) 
DO UPDATE SET
    book_ref = EXCLUDED.book_ref,
    passenger_id = EXCLUDED.passenger_id,
    flight_no = EXCLUDED.flight_no,
    scheduled_departure_date_local = EXCLUDED.scheduled_departure_date_local,
    scheduled_departure_date_utc = EXCLUDED.scheduled_departure_date_utc,
    scheduled_departure_time_local = EXCLUDED.scheduled_departure_time_local,
    scheduled_departure_time_utc = EXCLUDED.scheduled_departure_time_utc,
    scheduled_arrival_date_local = EXCLUDED.scheduled_arrival_date_local,
    scheduled_arrival_date_utc = EXCLUDED.scheduled_arrival_date_utc,
    scheduled_arrival_time_local = EXCLUDED.scheduled_arrival_time_local,
    scheduled_arrival_time_utc = EXCLUDED.scheduled_arrival_time_utc,
    departure_airport = EXCLUDED.departure_airport,
    arrival_airport = EXCLUDED.arrival_airport,
    aircraft_code = EXCLUDED.aircraft_code,
    status = EXCLUDED.status,
    fare_conditions = EXCLUDED.fare_conditions,
    seat_no = EXCLUDED.seat_no,
    updated_at = CASE WHEN 
                        final.fct_boarding_pass.book_ref <> EXCLUDED.book_ref
                        OR final.fct_boarding_pass.passenger_id <> EXCLUDED.passenger_id
                        OR final.fct_boarding_pass.flight_no <> EXCLUDED.flight_no
                        OR final.fct_boarding_pass.scheduled_departure_date_local <> EXCLUDED.scheduled_departure_date_local
                        OR final.fct_boarding_pass.scheduled_departure_date_utc <> EXCLUDED.scheduled_departure_date_utc
                        OR final.fct_boarding_pass.scheduled_departure_time_local <> EXCLUDED.scheduled_departure_time_local
                        OR final.fct_boarding_pass.scheduled_departure_time_utc <> EXCLUDED.scheduled_departure_time_utc
                        OR final.fct_boarding_pass.scheduled_arrival_date_local <> EXCLUDED.scheduled_arrival_date_local
                        OR final.fct_boarding_pass.scheduled_arrival_date_utc <> EXCLUDED.scheduled_arrival_date_utc
                        OR final.fct_boarding_pass.scheduled_arrival_time_local <> EXCLUDED.scheduled_arrival_time_local
                        OR final.fct_boarding_pass.scheduled_arrival_time_utc <> EXCLUDED.scheduled_arrival_time_utc
                        OR final.fct_boarding_pass.departure_airport <> EXCLUDED.departure_airport
                        OR final.fct_boarding_pass.arrival_airport <> EXCLUDED.arrival_airport
                        OR final.fct_boarding_pass.aircraft_code <> EXCLUDED.aircraft_code
                        OR final.fct_boarding_pass.status <> EXCLUDED.status
                        OR final.fct_boarding_pass.fare_conditions <> EXCLUDED.fare_conditions
                        OR final.fct_boarding_pass.seat_no <> EXCLUDED.seat_no
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.fct_boarding_pass.updated_at
                END;