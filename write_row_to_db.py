# External imports
import mariadb
import sys
import argparse
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


def write_row(database: str, what: str, temp_unit: str = 'C'):
    """
    Writes a row with the specified data to the specified database.

    Args:
        database (str): Specifies database to write to: 'dev_sensoric', 'test_sensoric' or 'prod_sensoric'
        what (str): Specifies what to read and write. 'all', 'bmp180_only' or 'pi_hw_only'
        temp_unit (str): Temperature unit of BMP180 measurement. 'C' for Celsius, 'F' for Fahrenheit or
            'K' for Kelvin. Use 'NO' to write Celsius data and skip writing unit information. (default='C')

    Returns:
        bool: True on Success

    """
    conn = None
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
            # cur.execute("SELECT * FROM dev_sensoric.test_table")
            # for row in cur:
            #     print(row)

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
                if temp_unit == 'F':
                    bmp180_temp = bmp180_temp * 9/5 + 32
                elif temp_unit == 'K':
                    bmp180_temp = bmp180_temp + 273.15
            except Exception as err:
                log.error("Failed to get BMP180 Data. Data is set to None.")
                log.error(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                bmp180_temp, bmp180_pressure = (None, None)

            # Write Data to Database
            try:
                # Write data into values table
                cur.execute(
                    f"""
                    INSERT INTO {database}.sensor_bmp180_values
                        (temperature, pressure) 
                        VALUES (?, ?);
                    """,
                    (bmp180_temp, bmp180_pressure)
                )

                conn.commit()
                value_pk = cur.lastrowid
                log.info(f"Inserted row into <sensor_bmp180_values> with ID: {value_pk}")

                # Write corresponding unit into units data
                if temp_unit != 'NO':
                    cur.execute(
                        f"""
                        INSERT INTO {database}.sensor_bmp180_units
                            (values_id, temperature_unit, pressure_unit) 
                            VALUES (?, ?, ?);
                        """,
                        (value_pk, temp_unit, 'hPa')
                    )

                    conn.commit()
                    log.info(f"Inserted row into <sensor_bmp180_units> with ID: {cur.lastrowid}")

            except (Exception, mariadb.Error) as err:
                log.fatal("Failed to write BMP 180 Sensor data to database.")
                log.fatal(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                raise

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
                    f"""
                    INSERT INTO {database}.pi_hw_monitor
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

                conn.commit()
                log.info(f"Inserted row into <pi_hw_monitor> with ID: {cur.lastrowid}")

            except (Exception, mariadb.Error) as err:
                log.fatal("Failed to write PI Hardware Info Data to database.")
                log.fatal(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
                raise

        log.info(f"write_row() successful! Parameters: database={database}, what={what}")

        return True

    except Exception as err:
        log.fatal("Unknown Error.")
        log.fatal(str(err) + "\n" + indent(text=traceback.format_exc(), prefix="\t"))
        raise
    finally:
        if conn:
            conn.close()
            log.info("Connection to MariaDB closed.")


if __name__ == '__main__':
    # Handle arguments
    parser = argparse.ArgumentParser(description='Write a row to the MariaDB Database')
    excl_group = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-d', '--database', metavar='database_name', type=str, required=True,
                        choices=('dev_sensoric', 'test_sensoric', 'prod_sensoric'),
                        help="Specifies database to write to. 'dev_sensoric', 'test_sensoric' or 'prod_sensoric'.")
    excl_group.add_argument('--all_data', action='store_true',
                            help="Specifies what to read and write. 'all_data' means bmp180 data AND pi hardware data.")
    excl_group.add_argument('--bmp180_only', action='store_true',
                            help="Specifies what to read and write. 'bmp180_only' means only bmp180 data.")
    excl_group.add_argument('--pi_hw_only', action='store_true',
                            help="Specifies what to read and write. 'pi_hw_only' means only pi hardware info data.")
    parser.add_argument('-tu', '--temp_unit', type=str, required=False, default='C',
                        choices=('C', 'F', 'K', 'NO'),
                        help="Unit to use for BMP180 Temperature. "
                             "'C' for Celsius, 'F' for Fahrenheit or 'K' for Kelvin. "
                             "Or 'NO' to write Celsius data without unit information")

    args = parser.parse_args()

    database_name = args.database
    if args.all_data:
        what_data = 'all'
    elif args.bmp180_only:
        what_data = 'bmp180_only'
    elif args.pi_hw_only:
        what_data = 'pi_hw_only'
    else:
        log.fatal("You have to choose at least one option between --all_data, --bmp180_only and --pi_hw_only!")
        raise AttributeError("You have to choose at least one option between"
                             " --all_data, --bmp180_only and --pi_hw_only!")

    log.info(f"Calling write_row() with parameters: database={database_name}, "
             f"what={what_data}, "
             f"temp_unit={args.temp_unit.upper()}")
    write_row(database=database_name, what=what_data, temp_unit=args.temp_unit.upper())
