WITH stg_dim_seats AS (
    SELECT
        s.id AS seat_id,
        da.aircraft_id AS aircraft_id,
        s.seat_no,
        s.fare_conditions
    FROM
        stg.seats s

    JOIN final.dim_aircraft da 
        ON da.aircraft_nk = s.aircraft_code

)

INSERT INTO final.dim_seat (
    seat_id,
    aircraft_id,
    seat_no,
    fare_conditions
)

SELECT
    seat_id,
    aircraft_id,
    seat_no,
    fare_conditions
    
FROM
    stg_dim_seats sds
    
ON CONFLICT(seat_id) 
DO UPDATE SET
    aircraft_id = EXCLUDED.aircraft_id,
    seat_no = EXCLUDED.seat_no,
    fare_conditions = EXCLUDED.fare_conditions,
    updated_at = CASE WHEN 
                        final.dim_seat.aircraft_id <> EXCLUDED.aircraft_id
                        OR final.dim_seat.seat_no <> EXCLUDED.seat_no
                        OR final.dim_seat.fare_conditions <> EXCLUDED.fare_conditions
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.dim_seat.updated_at
                END;