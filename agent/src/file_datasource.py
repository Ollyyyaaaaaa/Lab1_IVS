from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self._position = 0
        self._data = []

    def _smooth_gps(self, gps_generator):
        longitude1, latitude1 = map(float, next(gps_generator))
        yield longitude1, latitude1
        smooth_step = 5
        for p2 in gps_generator:
            longitude2, latitude2 = map(float, p2)
            for i in range(1, smooth_step):
                yield (
                    longitude1 + (longitude2 - longitude1) * i / smooth_step,
                    latitude1 + (latitude2 - latitude1) * i / smooth_step,
                )
            longitude1, latitude1 = longitude2, latitude2

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        if self._position == len(self._data):
            self._position = 0
        data = self._data[self._position]
        aggregated_data = AggregatedData(
            config.USER_ID,
            data.accelerometer,
            data.gps,
            datetime.now(),  # creation time
        )
        self._position += 1
        return aggregated_data

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self._position = 0
        self._data = []
        with open(self.accelerometer_filename, "r") as accelerometer_file:
            with open(self.gps_filename, "r") as gps_file:
                accelerometer_data_reader = reader(accelerometer_file)
                gps_data_reader = reader(gps_file)
                next(accelerometer_data_reader)
                next(gps_data_reader)

                for accelerometer_row, gps_row in zip(
                    accelerometer_data_reader, self._smooth_gps(gps_data_reader)
                ):
                    if not accelerometer_row or not gps_row:
                        continue
                    x, y, z = map(int, accelerometer_row)
                    longitude, latitude = map(float, gps_row)
                    aggregated_data = AggregatedData(
                        config.USER_ID,
                        Accelerometer(x, y, z),
                        Gps(longitude, latitude),
                        datetime.now(),
                    )
                    self._data.append(aggregated_data)

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        pass  # No action required for stopping reading
