import math
import pandas as pd
import numpy as np
import re
import json
from pathlib import Path
import sys


sys.path.append(str(Path(__file__).parent))


from common import Extractor
from common import AutosemRules, MeasureFeature, Measure


class MeasureExtractor(Extractor):
    def __init__(
        self,
        max_values: int = 0,
        add_space: bool = True,
    ):
        pass

    def _get_measures(self):
        if self.mode == "triplet":
            return Measures(self.measure_data).makeTriplets()
        elif self.mode == "overall":
            return Measures(self.measure_data).makeOverall()

    def _add_space(self, series: pd.Series) -> pd.Series:
        return series + " " if self.add_space else series

    def _show_status(self):
        print("Извлекаю характеристики типа:", self._name)

    def extract(self, data: pd.DataFrame, col: str) -> pd.DataFrame:
        """
        return dataframe with extra columns which depend of
        name of the measures (from MeasuresData attribute)
        """
        self._show_status()

        _data = self._add_space(data[col])
        measures = self._get_measures()

        for measure in measures:
            measureValues = self.tool.extractMeasureValues(
                _data,
                measure,
                max_values=self.max_values,
            )

            regex = self.tool.createMeasureRX(measureValues, measure)
            data = pd.concat([data, regex], axis=1)

        return data


class SizeExtractor(Extractor):
    def __init__(
        self,
        basic_sep: bool = True,
        custom_sep: str = r"[\/\\xх]",
        left_step: float = 10,
        right_step: float = 10,
        triple_from_double: bool = True,
        triple_from_double_pos: int = 0,
    ):
        self.basic_sep = basic_sep
        self.custom_sep = custom_sep
        self._kf1 = self._recount(left_step)
        self._kf2 = self._recount(1 / right_step)
        self.triple_from_double = triple_from_double
        self.triple_from_double_pos = triple_from_double_pos

        self._name = "_extr"

    def _show_status(self):
        print("Извлекаю размеры")

    def _extract_size_values(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        if self.basic_sep:
            sep = r"\D+"
        else:
            sep = self.custom_sep

        _int = r"\d*[.,]?\d+"
        rx_double = rf"({_int})(?:{sep})({_int})"
        rx_triple = rf"({_int})(?:{sep})({_int})(?:{sep})({_int})"

        df[self._name] = df[col].str.findall(rx_triple).str[0]
        df[self._name] = np.where(
            df[self._name].isna(),
            df[col].str.findall(rx_double).str[0],
            df[self._name],
        )

        return df

    def _triple_from_double(self, data: pd.DataFrame) -> pd.DataFrame:
        def get_values(series: pd.Series) -> pd.Series:
            if isinstance(series[self._doub], tuple):
                series["new_triple"] = (
                    series[self._doub][self.triple_from_double_pos],
                    series[self._doub][self.triple_from_double_pos],
                    series[self._doub][abs(self.triple_from_double_pos - 1)],
                )

            return series

        if self._triple_from_double:
            data[self._trip] = data[self._trip].apply(lambda x: x if x else np.nan)
            data = data.apply(get_values, axis=1)
            data[self._trip] = np.where(
                data[self._trip].isna(),
                data["new_triple"],
                data[self._trip],
            )
            data = data.drop("new_triple", axis=1)
        return data

    def _recount(self, val: float) -> float:
        if math.isclose(val, round(val)):
            val = round(val)
        return val

    def _prep_value(self, value: str, kf: float) -> float:
        value = self._recount(float(value) / kf)
        if value >= 1:
            # TODO - хардкод - может привести к ошибкам
            value = re.sub(r"\.0$", "", str(value))
        else:
            value = str(value)
        return value

    def _create_rx(self, values: tuple[int]) -> str:
        rx = ""
        kfs = [self._kf1, 1, self._kf2]

        if isinstance(values, tuple):
            for kf_ind in range(len(kfs)):
                _rx = "\D"
                kf = kfs[kf_ind]
                for val_ind in range(len(values)):
                    val = self._prep_value(values[val_ind], kf)
                    val_end = "\D+" if val_ind < len(values) - 1 else "\D"
                    _rx += rf"{val}{val_end}"

                _sep = "|" if kf_ind < len(kfs) - 1 else ""
                rx += _rx + _sep

        return rx

    def _create_trip_rx(self, values: tuple[int]) -> str:
        rx = ""
        if isinstance(values, tuple):
            kfs = [self._kf1, 1, self._kf2]
            for ind in range(len(kfs)):
                kf = kfs[ind]
                v1 = self._prep_value(values[0], kf)
                v2 = self._prep_value(values[1], kf)
                v3 = self._prep_value(values[2], kf)

                _rx = rf"\D{v1}\D+{v2}\D+{v3}\D"
                _sep = "|" if ind < len(kfs) - 1 else ""
                rx += _rx + _sep
            rx = "(" + rx + ")"

        return rx

    def _create_size_rx(self, data: pd.DataFrame) -> pd.DataFrame:
        data[self._name] = data[self._name].apply(self._create_rx)
        data["Sizes"] = "(?=.*(" + data[self._name] + "))"
        return data

    def _clean_up(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.drop(
            [
                self._name,
            ],
            axis=1,
        )

        data["Sizes"] = np.where(
            data["Sizes"] == "(?=.*(|))",
            "",
            data["Sizes"],
        )

        data["Sizes"] = np.where(
            data["Sizes"] == "(?=.*())",
            "",
            data["Sizes"],
        )

        return data

    def extract(self, data: pd.DataFrame, col: str) -> pd.DataFrame:
        self._show_status()

        data = self._extract_size_values(data, col)
        # data = self._triple_from_double(data) # TODO: rework it
        data = self._create_size_rx(data)  # ["Размеры"]
        data = self._clean_up(data)

        return data


if __name__ == "__main__":
    data = pd.DataFrame()
    data.at[0, "name"] = "Апельсины 100гр"
    data.at[1, "name"] = "Апельсины 1000кг"
