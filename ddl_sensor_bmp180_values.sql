CREATE TABLE IF NOT EXISTS prod_sensoric.sensor_bmp180_values
(
    id          BIGINT    NOT NULL AUTO_INCREMENT PRIMARY KEY ,
    ts_mod      TIMESTAMP NOT NULL,
    temperature FLOAT,
    pressure    FLOAT
)