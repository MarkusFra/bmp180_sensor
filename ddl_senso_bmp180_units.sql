CREATE TABLE IF NOT EXISTS prod_sensoric.sensor_bmp180_units
(
    id               BIGINT    NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ts_mod           TIMESTAMP NOT NULL,
    values_id        BIGINT    NOT NULL,
    temperature_unit VARCHAR(255),
    pressure_unit    VARCHAR(255),
    CONSTRAINT `fk_values_id`
        FOREIGN KEY (values_id) REFERENCES prod_sensoric.sensor_bmp180_values (id)
)
