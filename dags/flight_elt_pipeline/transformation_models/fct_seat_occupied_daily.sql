WITH 
    stg_flights AS (
        SELECT *
        FROM stg.flights
    ),

    dim_dates AS (
        SELECT *
        FROM final.dim_date
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
    
    stg_seats AS (
        SELECT *
        FROM stg.seats
    ),
    
    cnt_seat_occupied AS (
        SELECT
            sf.flight_id,
            count(seat_no) AS seat_occupied
        FROM stg_flights sf
        join stg_boarding_passes sbp 
            on sbp.flight_id = sf.flight_id
        where
            status = 'Arrived'
        group by 1
    ),
    
    cnt_total_seats AS (
        SELECT
            aircraft_code,
            count(seat_no) AS total_seat
        FROM stg_seats
        group by 1
    ),
    
    final_fct_seat_occupied_daily AS (
        SELECT 
            dd.date_id AS date_flight,
            sf.flight_id AS flight_nk,
            sf.flight_no AS flight_no,
            da1.airport_id AS departure_airport,
            da2.airport_id AS arrival_airport,
            dac.aircraft_id AS aircraft_code,
            sf.status AS status,
            cts.total_seat AS total_seat,
            cso.seat_occupied AS seat_occupied,
            (cts.total_seat - cso.seat_occupied) AS empty_seats
    
        FROM stg_flights sf
        join dim_dates dd 
            on dd.date_actual = DATE(sf.actual_departure)
        join dim_airports da1
            on da1.airport_nk = sf.departure_airport
        join dim_airports da2
            on da2.airport_nk = sf.arrival_airport
        join dim_aircrafts dac
            on dac.aircraft_nk = sf.aircraft_code
        join cnt_seat_occupied cso
            on cso.flight_id = sf.flight_id
        join cnt_total_seats cts 
            on cts.aircraft_code = sf.aircraft_code
    )
    
INSERT INTO final.fct_seat_occupied_daily(
    date_flight, 
    flight_nk, 
    flight_no, 
    departure_airport, 
    arrival_airport, 
    aircraft_code, 
    status, 
    total_seat, 
    seat_occupied, 
    empty_seats
)

SELECT 
    * 
FROM 
    final_fct_seat_occupied_daily

ON CONFLICT(date_flight, flight_nk) 
DO UPDATE SET
    flight_no = EXCLUDED.flight_no,
    departure_airport = EXCLUDED.departure_airport,
    arrival_airport = EXCLUDED.arrival_airport,
    aircraft_code = EXCLUDED.aircraft_code,
    status = EXCLUDED.status,
    total_seat = EXCLUDED.total_seat,
    seat_occupied = EXCLUDED.seat_occupied,
    empty_seats = EXCLUDED.empty_seats,
    updated_at = CASE WHEN 
                        final.fct_seat_occupied_daily.flight_no <> EXCLUDED.flight_no
                        OR final.fct_seat_occupied_daily.departure_airport <> EXCLUDED.departure_airport
                        OR final.fct_seat_occupied_daily.arrival_airport <> EXCLUDED.arrival_airport
                        OR final.fct_seat_occupied_daily.aircraft_code <> EXCLUDED.aircraft_code
                        OR final.fct_seat_occupied_daily.status <> EXCLUDED.status
                        OR final.fct_seat_occupied_daily.total_seat <> EXCLUDED.total_seat
                        OR final.fct_seat_occupied_daily.seat_occupied <> EXCLUDED.seat_occupied
                        OR final.fct_seat_occupied_daily.empty_seats <> EXCLUDED.empty_seats

                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.fct_seat_occupied_daily.updated_at
                END;