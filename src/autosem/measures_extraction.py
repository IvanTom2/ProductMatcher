from common import Extractor
from common import MeasureFuncTool, Measures

import math
import pandas as pd
import numpy as np
import re


class MeasuresData(object):
    """
    Contains data for any type of measures:
    - technique
    - concByMilliliter
    - concPercent
    - liquid
    - mass
    - ME
    """

    def __init__(self):
        pass

    @property
    def memory(self):
        _name = "Емкость памяти"
        prefix = ""
        postfix = ""
        measure_data = [
            {
                "name": "Килобайт",
                "symbols": r"kb|kilobite|килобайт|кб",
                "ratio": 0.000000001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Мегабайт",
                "symbols": r"mb|megabite|мегабай|мб",
                "ratio": 0.000001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Гигабайт",
                "symbols": r"gb|gigabite|гигабайт|гб",
                "ratio": 0.001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Терабайт",
                "symbols": r"tb|terabite|терабайт|тб",
                "ratio": 1,
                "prefix": prefix,
                "postfix": postfix,
            },
        ]
        return _name, measure_data

    @property
    def concByMilliliter(self):
        _name = "Концентрация в миллилитрах"
        prefix = ""
        postfix = ""
        measure_data = [
            {
                "name": "Мкг/мл",
                "symbols": r"мкг.мл",
                "ratio": 0.000001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Мг/мл",
                "symbols": r"мг.мл",
                "ratio": 0.001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "%содержания",
                "symbols": r"%",
                "ratio": 0.01,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Г/мл",
                "symbols": r"г.мл",
                "ratio": 1,
                "prefix": prefix,
                "postfix": postfix,
            },
        ]
        return _name, measure_data

    @property
    def concPercent(self):
        _name = "Концентрация только в процентах"
        prefix = r""
        postfix = r""
        measure_data = [
            {
                "name": "Процент",
                "symbols": r"%",
                "ratio": 1,
                "prefix": prefix,
                "postfix": postfix,
            },
        ]
        return _name, measure_data

    @property
    def liquid(self):
        _name = "Объем"
        prefix = r""
        postfix = r""
        measures_data = [
            {
                "name": "Миллилитр",
                "symbols": r"мл|миллилитр|ml|milliliter",
                "ratio": 0.001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Литр",
                "symbols": r"литр|л|liter|l",
                "ratio": 1,
                "prefix": prefix,
                "postfix": postfix,
            },
        ]
        return _name, measures_data

    @property
    def mass(self):
        _name = "Вес"
        prefix = r""
        postfix = r""
        measures_data = [
            {
                "name": "Микрограмм",
                "symbols": r"мкг|микрограмм|µg|microgram",
                "ratio": 0.000000001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Миллиграмм",
                "symbols": r"мг|миллиграмм|mg|milligram",
                "ratio": 0.000001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Грамм",
                "symbols": r"г|гр|грамм|g|gram",
                "ratio": 0.001,
                "prefix": prefix,
                "postfix": "[. ]",
            },
            {
                "name": "Килограмм",
                "symbols": r"кг|килограмм|kg|kilogram",
                "ratio": 1,
                "prefix": prefix,
                "postfix": postfix,
            },
        ]
        return _name, measures_data

    @property
    def ME(self):
        _name = "Международные единицы"
        prefix = r""
        postfix = r""
        measures_data = [
            {
                "name": "МЕ",
                "symbols": r"ме|iu|ед|me",
                "ratio": 0.000001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Тысяча МЕ",
                "symbols": r"тыс.ме|th.iu|тыс.ед|тыс.me|т.ме|th.iu|т.ед|т.me",
                "ratio": 0.001,
                "prefix": prefix,
                "postfix": postfix,
            },
            {
                "name": "Миллион МЕ",
                "symbols": r"млн.ме|mln.iu|млн.ед|млн.me",
                "ratio": 1,
                "prefix": prefix,
                "postfix": postfix,
            },
        ]
        return _name, measures_data

    def _getNames(self, measures):
        return [measure["name"] for measure in measures]

    def getNames(self):
        measures_types = [
            self.memory,
            self.concByMilliliter,
            self.concPercent,
            self.liquid,
            self.mass,
            self.ME,
        ]

        names = []
        for measures in measures_types:
            names.extend(self._getNames(measures[1]))
        return names


class MeasureExtractor(Extractor):
    """
    Extract measures from rows by measure rule and make regex for them.
    Example: 15 мл -> extract "15"

    - measure_data - MeasuresData attribute (for special measure)
    - mode - combining mode of regex, can be 'triplet' or 'overall'
    - max_values - max count of extracted values
    - add_space - add extra space in end of the rows (for better extration by regex)
    returning dataframe wouldn't contains the extra space
    """

    def __init__(
        self,
        measure_data: MeasuresData,
        mode: str = "triplet",
        max_values: int = 0,
        add_space: bool = True,
    ):
        self._name = measure_data[0]
        self.measure_data = measure_data[1]
        self.mode = mode
        self.max_values = max_values
        self.add_space = add_space

        self.tool = MeasureFuncTool()

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
