import json
import sys
import regex as re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Any
from decimal import Decimal
from collections import namedtuple
from typing import Union
from abc import abstractmethod

SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent

sys.path.append(str(PROJECT_DIR))
from config.measures_config.config_parser import (
    CONFIG,
    MEASURE,
    DATA,
    FEATURE,
    AUTOSEM_CONF,
)


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
    # also mode can be presented as number

    default = "overall"

    @classmethod
    def checkout(self, mode: str) -> str:
        mode = str(mode).lower()
        if mode not in self.modes:
            if not mode.isnumeric():
                mode = self.default
        return mode


class CommonValues(object):
    COMMON = "common"

    def is_common(self, value: Union[int, str]) -> bool:
        if value == self.COMMON:
            return True
        return False


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
        special_value_search: str,
    ) -> None:
        FD = feature_data
        CV = CommonValues()

        self.name = FD[FEATURE.NAME]
        self.defenition = FD[FEATURE.DEFENITION]
        self.relative_weight = Decimal(str(FD[FEATURE.RWEIGHT]))

        self.search_mode = (
            SearchMode.checkout(FD[FEATURE.SEARCH_MODE])
            if FEATURE.SEARCH_MODE in FD
            else SearchMode.default
        )

        self.prefix = (
            FD[FEATURE.PREFIX]
            if not CV.is_common(FD[FEATURE.PREFIX])
            else common_prefix
        )

        self.postfix = (
            FD[FEATURE.POSTFIX]
            if not CV.is_common(FD[FEATURE.POSTFIX])
            else common_postfix
        )

        self.max_count = (
            FD[FEATURE.MAX_COUNT]
            if not CV.is_common(FD[FEATURE.MAX_COUNT])
            else common_max_count
        )

        self._search_rx = self._make_search_rx(special_value_search)
        self.allocated_features = [self]

    def get_search_regex(self) -> str:
        return self._search_rx

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, self.__class__):
            if self.name == __value.name:
                return True
            else:
                return False
        else:
            return False

    @abstractmethod
    def _default_search(self):
        pass

    def _make_search_rx(self, special_value_search: str) -> str:
        DVS = self._default_search()  # default value search
        vsrch = DVS

        if special_value_search:
            vsrch = special_value_search

        if self.search_mode == SearchMode.BEHIND:
            rx = rf"{vsrch}\s*(?:{self.defenition})"
        else:
            rx = rf"(?:{self.defenition})\s*{vsrch}"

        return rx

    def _extract_values(self, string: pd.Series) -> list[str]:
        return re.findall(self._search_rx, string, re.IGNORECASE)

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

    def add_relative(self, features) -> None:
        self.allocated_features.extend(features)

    @abstractmethod
    def transform(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        pass

    def __repr__(self) -> str:
        _return = []
        _return.append(f"name: {self.name}")
        _return.append(f"defenition: {self.defenition}")
        _return.append(f"relative_weight: {self.relative_weight}")
        _return.append(f"prefix: '{self.prefix}'")
        _return.append(f"postfix: '{self.postfix}'\n")
        return "\n".join(_return)


class StringFeature(MeasureFeature):
    def _default_search(self):
        return ""

    def _to_regex(self, values: list[str]) -> list[str]:
        if values != []:
            values = [self._search_rx]
        return values

    def transform(
        self,
        extracted_values: list[list[str]],
    ) -> list[list[str]]:
        regex_values = map(self._to_regex, extracted_values)
        regex_values = map(lambda x: "".join(x), regex_values)
        regex_values = map(lambda x: "(?=.*(" + x + "))", regex_values)
        regex_values = map(lambda x: x.replace("(?=.*())", ""), regex_values)

        return list(regex_values)


class NumericFeature(MeasureFeature):
    def _default_search(self):
        return "\d*[.,]?\d+"

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
                if "." in num:
                    num = re.sub("[.]", "[.,]", num)
        return num

    def _to_regex(self, numeric_values: list[str]) -> list[str]:
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
                        + feature.postfix
                    )

                rx_parts.append(rx_part)
                worked_features.add(feature.name)

            regexes.append("|".join(rx_parts))

        return regexes

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


class FeatureType(object):
    NUMERIC = "numeric_feature"
    STRING = "string_feature"

    __mapper = {
        CONFIG.NUMERIC_MEASURES: NUMERIC,
        CONFIG.STRING_MEASURES: STRING,
    }

    __type = {
        NUMERIC: NumericFeature,
        STRING: StringFeature,
    }

    def __init__(self, measure_type: str) -> None:
        self._type = self.__mapper[measure_type]

    def type(self) -> MeasureFeature:
        return self.__type[self._type]


class Measure(object):
    def __init__(
        self,
        measure_name: str,
        merge_mode: str,
        measure_data: dict,
        measure_type: str,
        exclude_rx: bool = False,
    ) -> None:
        self.name = measure_name
        self.merge_mode = merge_mode
        self.measure_type = measure_type
        self.exclude_rx = exclude_rx

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
        special_value_search = measure_data[DATA.SPECIAL_VALUE_SEARCH]
        feature_type = FeatureType(self.measure_type)

        features = []
        for feature_data in measure_data[DATA.FEATURES]:
            if feature_data[FEATURE.USE_IT]:
                type = feature_type.type()

                features.append(
                    type(
                        feature_data,
                        common_prefix,
                        common_postfix,
                        common_max_count,
                        special_value_search,
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

    def _make_exclude_rx(self) -> pd.DataFrame:
        behind = ""
        front = ""
        for feature in self.features:
            search_mode = feature.search_mode
            if search_mode == SearchMode.BEHIND:
                behind += feature.defenition
            else:
                front += feature.defenition

        rx = "^(?!.*("
        if behind:
            rx += "(?:[0-9][0-9]\d*|[2-9]\d*?)\s*" + "(?:" + behind + ")"

        if front:
            if behind:
                rx += "|"
            rx += "(?:" + front + ")" + "\s*(?:[0-9][0-9]\d*|[2-9]\d*)"

        rx += "))"
        return rx

    def _add_exclude_rx(
        self,
        data: pd.DataFrame,
        feature_names: list[str],
        new_feature_name: str,
    ) -> pd.DataFrame:
        exclude_rx = self._make_exclude_rx()

        data[new_feature_name] = np.where(
            data[feature_names].apply(lambda r: r.str.strip().eq("").all(), axis=1),
            exclude_rx,
            "",
        )

        return data

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

        if self.exclude_rx:
            new_feature_name = "Исключ. " + self.name
            data = self._add_exclude_rx(data, feature_names, new_feature_name)
            feature_names.append(new_feature_name)

        return data, feature_names


class Measures(object):
    """
    Measures container class for manage and control regex creation

    Parameters
    ----------
    config : dict
        Parsed config file for usage

    """

    def __init__(self, config: dict) -> None:
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

    def _create_measures(self, config: dict) -> dict[str, Measure]:
        """Parse config and create dict with Measure objects
        accorging to parsed rules"""

        measures_list = {}
        for MEASURE_TYPE in CONFIG.MEASURE_TYPES:
            if config[MEASURE_TYPE][CONFIG.USE_IT]:
                data = config[MEASURE_TYPE]

                for measure_record in data[CONFIG.MEASURES]:
                    if MEASURE.NAME in measure_record:
                        autosem_conf = measure_record[MEASURE.AUTOSEM]

                        if autosem_conf[AUTOSEM_CONF.USE_IT]:
                            measures_list[measure_record[MEASURE.NAME]] = Measure(
                                measure_record[MEASURE.NAME],
                                autosem_conf[AUTOSEM_CONF.MERGE_MODE],
                                measure_record[MEASURE.DATA],
                                MEASURE_TYPE,
                                autosem_conf[AUTOSEM_CONF.EXCLUDE_RX],
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


def read_config(path: str):
    with open(path, "rb") as file:
        data = json.loads(file.read())
    return data


def fast_test():
    data = pd.DataFrame()
    data.at[0, "name"] = "Апельсины 100гр"
    data.at[1, "name"] = "Апельсины 10кг"
    data.at[2, "name"] = "Вода 1000мл 40мл"
    data.at[3, "name"] = "Сок 1л"
    data.at[4, "name"] = "Пиво 500мл 600мл"
    data.at[5, "name"] = "Бананы 10шт"
    data.at[6, "name"] = "Аспирин 10шт"
    data.at[6, "name"] = "Аспирин №10"
    data.at[7, "name"] = "Аспирин №1"
    data.at[8, "name"] = "Аспирин 1шт"
    data.at[9, "name"] = "Пальто зеленое"
    data.at[10, "name"] = "Пальто красное"
    data.at[11, "name"] = "Пальто синее"
    data.at[12, "name"] = "Пальто красно-синее"

    config = read_config(
        "/home/mainus/Projects/ProductMatcher/config/measures_config/setups/main.json"
    )

    measures = Measures(config)
    data = measures.extract_all(data, "name")
    data = measures.concat_regex(data, True)

    print(data)
    print(data.at[0, "regex"])
    print(data.at[1, "regex"])
    print(data.at[2, "regex"])
    print(data.at[3, "regex"])


if __name__ == "__main__":
    data = pd.DataFrame()
    data.at[0, "name"] = "Мандарин 10шт"
    data.at[1, "name"] = "Мандарин 11шт"
    data.at[2, "name"] = "Мандарин 12шт"
    data.at[3, "name"] = "Мандарин 1шт"

    config = read_config(
        "/home/mainus/Projects/ProductMatcher/config/measures_config/setups/main.json"
    )

    print(config)

    # measures = Measures(config)
    # data = measures.extract_all(data, "name")
    # data = measures.concat_regex(data, True)
