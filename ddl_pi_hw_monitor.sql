CREATE TABLE IF NOT EXISTS prod_sensoric.pi_hw_monitor
(
    id              int       NOT NULL AUTO_INCREMENT,
    ts_mod          timestamp NOT NULL,
    nr_of_processes INT,
    cpu_usage       FLOAT,
    cpu_frequency   INT,
    cpu_temperature FLOAT,
    cpu_fan_bpm     FLOAT,
    ram_usage       FLOAT,
    disk_usage_free BIGINT,
    disk_usage_used BIGINT,
    PRIMARY KEY (id)
)

