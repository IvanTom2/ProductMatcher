import regex as re
from typing import Any
import pandas as pd
import json
from pathlib import Path


from decimal import Decimal
from collections import namedtuple
from typing import Union


class MeasureFeature(object):
    def __init__(
        self,
        feature_data: dict,
        common_prefix: str,
        common_postfix: str,
    ) -> None:
        FD = feature_data

        self.name = FD["feature_name"]
        self.defenition = FD["defenition"]
        self.relative_weight = FD["relative_weight"]

        self.search_mode = FD["search_mode"] if "search_mode" in FD else "post"
        self.prefix = FD["prefix"] if "prefix" in FD else common_prefix
        self.postfix = FD["postfix"] if "postfix" in FD else common_postfix

        self._search_rx = self._make_search_rx()
        self.relative_features = []

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, self.__class__):
            if self.name == __value.name:
                return True
            else:
                return False
        else:
            return False

    def _make_search_rx(self) -> str:
        if self.search_mode == "post":
            rx = f"\d*[.,]?\d+\s*(?:{self.defenition})"
        else:
            rx = f"(?:{self.defenition})\s*\d*[.,]?\d+"

        return rx

    def extract(
        self,
        data: pd.DataFrame,
        column: str,
    ) -> pd.Series:
        values = data[column].apply(
            re.findall,
            args=(self._search_rx,),
        )
        return values

    def add_relative(self, features) -> None:
        self.relative_features.extend(features)

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

        self.features = self._parse_measure_data(measure_data)
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

    def _parse_measure_data(self, measure_data: dict) -> list[MeasureFeature]:
        common_prefix = measure_data["common_prefix"]
        common_postfix = measure_data["common_postfix"]

        features = []
        for feature_data in measure_data["features"]:
            features.append(
                MeasureFeature(
                    feature_data,
                    common_prefix,
                    common_postfix,
                )
            )

        return features

    def _sort_features(self) -> None:
        self.features.sort(key=lambda ft: ft.relative_weight)

    def _allocate_relative_features(self) -> None:
        if self.merge_mode in ("o", "overall"):
            for feature in self.features:
                other_features = [f for f in self.features if f is not feature]
                feature.add_relative(other_features)

        else:
            shift = int(self.merge_mode)
            for index in range(len(self.features)):
                feature = self.features[index]
                left = self.features[max(0, index - shift - 1) : index - 1]
                right = self.features[index : min(len(self.features), index + shift)]

                other_features = left + right
                other_features = [f for f in self.features if f is not feature]

                feature.add_relative(other_features)

    def extract(
        self,
        data: pd.DataFrame,
        column: str,
    ) -> pd.Series:
        for feature in self.features:
            values = feature.extract(data, column)
            print(values)

        return data


class Measures(object):
    """
    config_name - name of config file without .json
    measures_names - list of measures names for usage
    """

    __PATH = Path(__file__).parent.parent.parent / "autosem_config"

    def __init__(
        self,
        config_name: str = "main",
    ) -> None:
        self.name = config_name

        self._config_path = self.__PATH / f"{config_name}.json"
        config = self._read_config()

        self.measures_names = self._extract_measures_names(config)
        self.measures = self._parse_rules(config)

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
        with open(self._config_path, "rb") as file:
            rules = file.read()
        return json.loads(rules)

    def _extract_measures_names(self, config: dict) -> list[str]:
        measures = []
        measures_records = config["measures"]
        for mrecord in measures_records:
            if "measure_name" in mrecord:
                measures.append(mrecord["measure_name"])
        return measures

    def _parse_rules(self, config: dict) -> dict[str, Measure]:
        measures_list = {}
        for measure in config["measures"]:
            measures_list[measure["measure_name"]] = Measure(
                measure["measure_name"],
                measure["merge_mode"],
                measure["measure_data"],
            )

        return measures_list

    def extract_all(
        self,
        data: pd.DataFrame,
        column: str,
    ) -> pd.DataFrame:
        for measure_name in self.measures_names:
            measure = self.measures[measure_name]

            data = measure.extract(data, column)

        return data


if __name__ == "__main__":
    data = pd.DataFrame()
    data.at[0, "name"] = "Апельсины 100гр"
    data.at[1, "name"] = "Апельсины 10кг"
    data.at[2, "name"] = "Вода 1000мл"
    data.at[3, "name"] = "Сок 1л"
    data.at[4, "name"] = "Пиво 500мл"

    measures = Measures("main")
    data = measures.extract_all(data, "name")
