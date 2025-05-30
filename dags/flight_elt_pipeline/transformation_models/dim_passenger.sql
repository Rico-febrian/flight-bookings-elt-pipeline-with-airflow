INSERT INTO final.dim_passenger (
    passenger_id,
    passenger_nk,
    passenger_name,
    phone,
    email
)

SELECT
    t.id AS passenger_id,
    t.passenger_id AS passenger_nk,
    t.passenger_name,
    t.contact_data->>'phone' AS phone,
    t.contact_data->>'email' AS email
    
FROM
    stg.tickets t 
    
ON CONFLICT(passenger_id) 
DO UPDATE SET
    passenger_nk = EXCLUDED.passenger_nk,
    passenger_name = EXCLUDED.passenger_name,
    phone = EXCLUDED.phone,
    email = EXCLUDED.email,
    updated_at = CASE WHEN 
                        final.dim_passenger.passenger_nk <> EXCLUDED.passenger_nk
                        OR final.dim_passenger.passenger_name <> EXCLUDED.passenger_name
                        OR final.dim_passenger.phone <> EXCLUDED.phone
                        OR final.dim_passenger.email <> EXCLUDED.email
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.dim_passenger.updated_at
                END;