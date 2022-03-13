# External imports
import mariadb
import sys
from logging import getLogger, StreamHandler, Formatter
from logging.handlers import TimedRotatingFileHandler

# Internal imports
from pi_monitoring import get_fan_rpm, get_cpu_data, get_ram_info, get_disk_info
from read_sensor import get_bmp180_data


# Logging configuration
log = getLogger(__name__)
log_formatter = Formatter(fmt="[%(asctime)s | %(name)s.%(funcName)s | %(levelname)s]: %(message)s",
                          datefmt="%Y.%m.%d %H:%M%S %z")
s_handler = StreamHandler()
log.addHandler(s_handler)
log.error("test")

raise ZeroDivisionError

def write_row():
    # First get data to write
    # Get data from Boschs BMP180 Sensor
    try:
        bmp180_temp, bmp180_pressure, _ = get_bmp180_data()
    except Exception as err:
        print("ERROR: Failed to get BMP180 Data.")
    # Get infos from raspberry pi system via psutil
    nr_of_processes, cpu_usage, cpu_frequency, cpu_temp = get_cpu_data()
    ram_usage = get_ram_info()
    disk_usage_free, disk_usage_used = get_disk_info()
    # Get fan RPM from Power Hat via GPIO
    fan_rpm = get_fan_rpm()

    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user="tu_rasppi",
            password="dbc497808bd013df63e2ddd68d6ec7cf",
            host="localhost",
            port=3306,
            database=""

        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    # Get Cursor
    cur = conn.cursor()

# Test SELECT
cur.execute("SELECT * FROM dev_sensoric.test_table")

for row in cur:
    print(row)

