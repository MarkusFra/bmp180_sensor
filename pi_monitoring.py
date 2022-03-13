import psutil
import RPi.GPIO as GPIO
import time

from typing import Tuple


def get_fan_rpm() -> float:
    """

    Returns:
        cpu_fan_rpm

    """
    TACH = 16
    PULSE = 2
    WAIT_TIME = 3

    GPIO.setmode(GPIO.BCM)
    GPIO.setwarnings(False)
    GPIO.setup(TACH, GPIO.IN, pull_up_down=GPIO.PUD_UP)

    t = time.time()
    rpm = 0

    def fell(n):
        nonlocal t
        nonlocal rpm

        dt = time.time() - t
        if dt < 0.005: return

        freq = 1 / dt
        rpm = (freq / PULSE) * 60
        t = time.time()

    GPIO.add_event_detect(TACH, GPIO.FALLING, fell)

    try:
        time.sleep(1)
        return rpm

    finally:
        GPIO.cleanup()


def get_cpu_data() -> Tuple[int, float, float, float]:
    """

    Returns:
        nr_of_processes, cpu_usage, cpu_frequency, cpu_temp

    """

    return \
        len(psutil.pids()), \
        psutil.cpu_percent(), \
        psutil.cpu_freq()[0], \
        psutil.sensors_temperatures()['cpu_thermal'][0][1]


def get_ram_info() -> float:
    """

    Returns:
        ram_usage

    """

    return psutil.virtual_memory()[2]


def get_disk_info() -> Tuple[int, int]:
    """

    Returns:
        disk_usage_free, disk_usage_used

    """

    return \
        psutil.disk_usage('/mnt/sda1')[2], \
        psutil.disk_usage('/mnt/sda1')[1]


print(get_cpu_data())
print(get_fan_rpm())
