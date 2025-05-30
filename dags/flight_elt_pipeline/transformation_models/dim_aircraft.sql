INSERT INTO final.dim_aircraft (
    aircraft_id,
    aircraft_nk,
    model,
    range
)

SELECT
    ad.id AS aircraft_id,
    ad.aircraft_code AS aircraft_nk,
    ad.model,
    ad.range
    
FROM
    stg.aircrafts_data ad 
    
ON CONFLICT(aircraft_id) 
DO UPDATE SET
    aircraft_nk = EXCLUDED.aircraft_nk,
    model = EXCLUDED.model,
    range = EXCLUDED.range,
    updated_at = CASE WHEN 
                        final.dim_aircraft.aircraft_nk <> EXCLUDED.aircraft_nk
                        OR final.dim_aircraft.model <> EXCLUDED.model
                        OR final.dim_aircraft.range <> EXCLUDED.range
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.dim_aircraft.updated_at
                END;