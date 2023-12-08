import regex as re
from typing import Any
import pandas as pd
import json
from pathlib import Path
from abc import ABC
import sys
from decimal import Decimal
from collections import namedtuple
from typing import Union
from enum import Enum

sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent))

from autosem_config.config_parser import CONFIG, MEASURE, DATA, FEATURE


class SearchMode(object):
    FRONT = "front"
    BEHIND = "behind"
    modes = {FRONT, BEHIND}

    default = BEHIND

    @classmethod
    def checkout(self, mode: str) -> str:
        mode = str(mode).lower()
        if mode not in self.modes:
            mode = self.default
        return mode


class MergeMode(object):
    NONE = "none"
    OVERALL = "overall"
    modes = {OVERALL, NONE}

    default = "overall"

    @classmethod
    def checkout(self, mode: str) -> str:
        mode = str(mode).lower()
        if mode not in self.modes:
            if not mode.isnumeric():
                mode = self.default
        return mode


class AbstractMeasureFeature(object):
    name: str
    defenition: str
    relative_weight: str
    search_mode: str
    prefix: str
    postfix: str


class MeasureFeature(AbstractMeasureFeature):
    def __init__(
        self,
        feature_data: dict,
        common_prefix: str,
        common_postfix: str,
        common_max_count: str,
    ) -> None:
        FD = feature_data

        self.name = FD[FEATURE.NAME]
        self.defenition = FD[FEATURE.DEFENITION]
        self.relative_weight = Decimal(str(FD[FEATURE.RWEIGHT]))

        self.search_mode = (
            SearchMode.checkout(FD[FEATURE.SEARCH_MODE])
            if FEATURE.SEARCH_MODE in FD
            else SearchMode.default
        )
        self.prefix = FD[FEATURE.PREFIX] if FEATURE.PREFIX in FD else common_prefix
        self.postfix = FD[FEATURE.POSTFIX] if FEATURE.POSTFIX in FD else common_postfix
        self.max_count = (
            FD[FEATURE.MAX_COUNT] if FEATURE.MAX_COUNT in FD else common_max_count
        )

        self._search_rx = self._make_search_rx()
        self.allocated_features = [self]

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, self.__class__):
            if self.name == __value.name:
                return True
            else:
                return False
        else:
            return False

    def _make_search_rx(self) -> str:
        if self.search_mode == SearchMode.BEHIND:
            rx = rf"\d*[.,]?\d+\s*(?:{self.defenition})"
        else:
            rx = rf"(?:{self.defenition})\s*\d*[.,]?\d+"

        return rx

    def _extract_values(self, string: pd.Series) -> list[str]:
        return re.findall(self._search_rx, string, re.IGNORECASE)

    def _extract_numeric_values(self, values: list[str]) -> list[str]:
        numeric_values = []
        for value in values:
            searched = re.search(r"\d*[.,]?\d+", value, re.IGNORECASE)
            if searched:
                numeric_values.append(Decimal(searched[0]))

        return numeric_values

    def _prepare_num(self, num: str) -> str:
        if num and not isinstance(num, list):
            num = f"{num:.20f}"
            if isinstance(num, str):
                if "." in num:
                    num = re.sub("0+$", "", num)
                    num = re.sub("\.$", "", num)
        return num

    def _to_regex(self, numeric_values: str) -> str:
        regexes = []
        features: list[AbstractMeasureFeature] = self.allocated_features

        worked_features = set()

        for numeric_value in numeric_values:
            rx_parts = []
            for feature in features:
                num: Decimal = numeric_value * (
                    self.relative_weight / feature.relative_weight
                )
                num = self._prepare_num(num)

                if feature.search_mode == SearchMode.BEHIND:
                    rx_part = (
                        feature.prefix
                        + "\D"
                        + num
                        + "\s*"
                        + "(?:"
                        + feature.defenition
                        + ")"
                        + feature.postfix
                    )

                else:
                    rx_part = (
                        feature.prefix
                        + "(?:"
                        + feature.defenition
                        + ")"
                        + "\s*"
                        + num
                        + "\D"
                        + feature.postfix
                    )

                rx_parts.append(rx_part)
                worked_features.add(feature.name)

            regexes.append("|".join(rx_parts))

        return regexes

    def extract(
        self,
        extract_from: list[str],
    ) -> list[list[str]]:
        values = list(map(self._extract_values, extract_from))
        return values

    def filter_count(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        if self.max_count != None:
            extracted_values = list(
                map(
                    lambda x: x[: self.max_count],
                    extracted_values,
                )
            )

        return extracted_values

    def transform(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        numeric_values = map(
            self._extract_numeric_values,
            extracted_values,
        )

        regex_values = map(self._to_regex, numeric_values)
        regex_values = map(lambda x: "))(?=.*(".join(x), regex_values)
        regex_values = map(lambda x: "(?=.*(" + x + "))", regex_values)
        regex_values = map(lambda x: x.replace("(?=.*())", ""), regex_values)

        return list(regex_values)

    def add_relative(self, features) -> None:
        self.allocated_features.extend(features)

    def __repr__(self) -> str:
        _return = []
        _return.append(f"name: {self.name}")
        _return.append(f"defenition: {self.defenition}")
        _return.append(f"relative_weight: {self.relative_weight}")
        _return.append(f"prefix: '{self.prefix}'")
        _return.append(f"postfix: '{self.postfix}'\n")
        return "\n".join(_return)


class Measure(object):
    def __init__(
        self,
        measure_name: str,
        merge_mode: str,
        measure_data: dict,
    ) -> None:
        self.name = measure_name
        self.merge_mode = merge_mode

        self.features = self._create_features(measure_data)
        self._sort_features()
        self._allocate_relative_features()

    def __iter__(self):
        self.__i = 0
        return self

    def __next__(self) -> MeasureFeature:
        if self.__i >= len(self.features):
            raise StopIteration
        else:
            item = self.features[self.__i]
            self.__i += 1
            return item

    def __len__(self) -> int:
        return len(self.features)

    def __getitem__(self, index: int) -> MeasureFeature:
        if index >= len(self.features):
            raise IndexError()
        else:
            return self.features[index]

    def __repr__(self) -> str:
        return self.name

    def _create_features(self, measure_data: dict) -> list[MeasureFeature]:
        common_prefix = measure_data[DATA.COMMON_PREFIX]
        common_postfix = measure_data[DATA.COMMON_POSTFIX]
        common_max_count = measure_data[DATA.COMMON_MAX_COUNT]

        features = []
        for feature_data in measure_data[DATA.FEATURES]:
            if feature_data[FEATURE.USE_IT]:
                features.append(
                    MeasureFeature(
                        feature_data,
                        common_prefix,
                        common_postfix,
                        common_max_count,
                    )
                )

        return features

    def _sort_features(self) -> None:
        self.features.sort(key=lambda ft: ft.relative_weight)

    def _allocate_relative_features(self) -> None:
        if self.merge_mode == MergeMode.NONE:
            pass

        elif self.merge_mode == MergeMode.OVERALL:
            for feature in self.features:
                other_features = [f for f in self.features if f != feature]
                feature.add_relative(other_features)

        else:
            shift = int(self.merge_mode)
            for index in range(len(self.features)):
                feature = self.features[index]
                maxlen = len(self.features)

                left = self.features[max(0, index - shift) : index]
                right = self.features[
                    min(index + 1, maxlen) : min(index + shift + 1, maxlen)
                ]

                other_features = left + right
                other_features = [f for f in other_features if f != feature]

                feature.add_relative(other_features)

    def extract(
        self,
        data: pd.DataFrame,
        column: str,
    ) -> pd.DataFrame:
        feature_names = []
        extract_from = data[column].to_list()

        for feature in self.features:
            extracted_values = feature.extract(extract_from)
            extracted_values = feature.filter_count(extracted_values)
            rx_patterns = feature.transform(extracted_values)

            data[feature.name] = rx_patterns
            feature_names.append(feature.name)

        return data, feature_names


class Measures(object):
    """
    Measures container class for manage and control regex creation

    Parameters
    ----------
    config_name : str
        Name of config file for usage (don't pass path, only filename)

    """

    __PATH = Path(__file__).parent.parent / "autosem_config"

    def __init__(
        self,
        config_name: str = "main",
    ) -> None:
        self.name = config_name

        self._config_path = self.__PATH / f"{config_name}.json"
        config = self._read_config()

        self.measures = self._create_measures(config)
        self.measures_names = list(self.measures.keys())

        self.used_feature_names = []

    def __iter__(self):
        self.__iterbale = list(self.measures.keys())
        self.__i = 0
        return self

    def __next__(self) -> Measure:
        if self.__i >= len(self.measures):
            raise StopIteration
        else:
            key = self.__iterbale[self.__i]
            item = self.measures[key]
            self.__i += 1
            return item

    def __len__(self) -> int:
        return len(self.measures)

    def __getitem__(self, subscript: Union[int, str]) -> Measure:
        if isinstance(subscript, int):
            if subscript >= len(self.measures):
                raise IndexError
            else:
                key = list(self.measures.keys())[subscript]
                return self.measures[key]

        else:
            if subscript in self.measures:
                return self.measures[subscript]
            else:
                raise KeyError

    def _read_config(self) -> dict:
        """Returns config file like dict object"""

        with open(self._config_path, "rb") as file:
            rules = file.read()
        return json.loads(rules)

    def _create_measures(self, config: dict) -> dict[str, Measure]:
        """Parse config and create dict with Measure objects
        accorging to parsed rules"""

        measures_list = {}
        for measure_record in config[CONFIG.MEASURES]:
            if MEASURE.NAME in measure_record:
                if measure_record[MEASURE.USE_IT]:
                    measures_list[measure_record[MEASURE.NAME]] = Measure(
                        measure_record[MEASURE.NAME],
                        measure_record[MEASURE.MERGE_MODE],
                        measure_record[MEASURE.DATA],
                    )

        return measures_list

    def extract_measure(
        self,
        data: pd.DataFrame,
        column: str,
        measure_name: str,
    ) -> pd.Series:
        """Extract regex by measure name"""

        measure = self.measures[measure_name]
        data, feature_names = measure.extract(data, column)

        extracted = data[feature_names[0]]
        if len(feature_names) >= 2:
            for feature_name in feature_names[1:]:
                extracted += data[feature_name]

        return extracted

    def extract_all(
        self,
        data: pd.DataFrame,
        column: str,
    ) -> pd.DataFrame:
        """Extract regex for all measures"""

        for measure_name in self.measures_names:
            measure = self.measures[measure_name]
            data, feature_names = measure.extract(data, column)

            self.used_feature_names.extend(feature_names)

        return data

    def concat_regex(
        self,
        data: pd.DataFrame,
        delete_features_columns: bool = False,
    ) -> pd.DataFrame:
        data["regex"] = ""
        for feature_name in self.used_feature_names:
            data["regex"] += data[feature_name]

        if delete_features_columns:
            data = data.drop(self.used_feature_names, axis=1)

        return data


if __name__ == "__main__":
    data = pd.DataFrame()
    data.at[0, "name"] = "Апельсины 100гр"
    data.at[1, "name"] = "Апельсины 10кг"
    data.at[2, "name"] = "Вода 1000мл 40мл"
    data.at[3, "name"] = "Сок 1л"
    data.at[4, "name"] = "Пиво 500мл 600мл"
    data.at[5, "name"] = "Бананы 10шт"
    data.at[6, "name"] = "Аспирин №10"

    measures = Measures("main")
    data = measures.extract_all(data, "name")
    data = measures.concat_regex(data, True)

    print(data.at[6, "regex"])
    print(data)

    # check = measures.extract_measure(data, "name", "Количество")
    # print(check[6])
