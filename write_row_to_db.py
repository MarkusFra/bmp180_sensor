# External imports
import mariadb
import sys
from logging import getLogger, StreamHandler, Formatter
from logging.handlers import TimedRotatingFileHandler
import traceback
from textwrap import indent

# Internal imports
from pi_monitoring import get_fan_rpm, get_cpu_data, get_ram_info, get_disk_info
from read_sensor import get_bmp180_data

# Logging configuration
log = getLogger(__name__)
log_formatter = Formatter(fmt="[%(asctime)s | %(levelname)s | %(name)s.%(funcName)s]: %(message)s",
                          datefmt="%Y.%m.%d %H:%M:%S%z")
log.setLevel("DEBUG")
s_handler = StreamHandler()
s_handler.setFormatter(log_formatter)
s_handler.setLevel(level='INFO')
f_handler = TimedRotatingFileHandler(filename='./log/write_row_to_db.log', when='midnight')
f_handler.setFormatter(log_formatter)
f_handler.setLevel(level='DEBUG')
log.addHandler(s_handler)
log.addHandler(f_handler)


def write_row(database: str, what: str):
    """
    Writes a row with the specified data to the specified database.

    Args:
        database (str): Specifies database to write to: 'dev_sensoric', 'test_sensoric' or 'prod_sensoric'
        what (str): Specifies what to read and write. 'all', 'bmp180_only' or 'pi_hw_only'

    Returns:
        bool: True on Success

    """
    # Connect to MariaDB Server
    try:
        try:
            conn = mariadb.connect(
                user="tu_rasppi",
                password="dbc497808bd013df63e2ddd68d6ec7cf",
                host="localhost",
                port=3306,
                database=""
            )

            # Get Cursor
            cur = conn.cursor()

            # for debugging
            cur.execute("SELECT * FROM dev_sensoric.test_table")
            for row in cur:
                print(row)

        except mariadb.Error as err:
            log.fatal("Failed to connect to Database. No attempt to write data will be done.")
            log.fatal(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
            raise

        log.info("Connection to MariaDB established successfully!")

        # Get data to write

        # Get data from Boschs BMP180 Sensor
        if what in ('all', 'bmp180_only'):
            try:
                bmp180_temp, bmp180_pressure, _ = get_bmp180_data()
            except Exception as err:
                log.error("Failed to get BMP180 Data. Data is set to None.")
                log.error(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                bmp180_temp, bmp180_pressure = (None, None)

        # Get infos from raspberry pi system via psutil
        if what in ('all', 'pi_hw_only'):
            # Get CPU Info
            try:
                nr_of_processes, cpu_usage, cpu_frequency, cpu_temp = get_cpu_data()
            except Exception as err:
                log.error("Failed to get CPU Info Data. Data is set to None.")
                log.error(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                nr_of_processes, cpu_usage, cpu_frequency, cpu_temp = (None, None, None, None)

            # Get RAM data
            try:
                ram_usage = get_ram_info()
            except Exception as err:
                log.error("Failed to get RAM Info Data. Data is set to None.")
                log.error(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                ram_usage = None

            try:
                disk_usage_free, disk_usage_used = get_disk_info()
            except Exception as err:
                log.error("Failed to get Disk Info Data. Data is set to None.")
                log.error(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                disk_usage_free, disk_usage_used = (None, None)

            # Get fan RPM from Power Hat via GPIO
            try:
                fan_rpm = get_fan_rpm()
            except Exception as err:
                log.error("Failed to get CPU Fan Data. Data is set to None.")
                log.error(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                fan_rpm = None

            # Write Data to Database
            try:
                cur.execute(
                    """
                    INSERT INTO dev_sensoric.pi_hw_monitor
                        (nr_of_processes, 
                        cpu_usage, 
                        cpu_frequency, 
                        cpu_temperature, 
                        cpu_fan_rpm, 
                        ram_usage, 
                        disk_usage_free, 
                        disk_usage_used) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (nr_of_processes,
                     cpu_usage,
                     cpu_frequency,
                     cpu_temp,
                     fan_rpm,
                     ram_usage,
                     disk_usage_free,
                     disk_usage_used)
                )

            except (Exception, mariadb.Error) as err:
                log.fatal("Failed to PI Hardware Info Data to database.")
                log.fatal(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                raise

        log.info(f"write_row() successful! Parameters: database={database}, what={what}")

        return True

    except Exception as err:
        log.fatal("Unknown Error.")
        log.fatal(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
        raise


if write_row(database='dev_sensoric', what='pi_hw_only'):
    print("SUCCESS")
