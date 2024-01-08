import sys
import warnings

import regex as re
import pandas as pd

from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path
from tqdm import tqdm

tqdm.pandas()
sys.path.append(str(Path(__file__).parent.parent))

from notation import FEATURES
from src.feature_v.feature_generator import FeatureGenerator
from src.feature_v.feature_functool import (
    AbstractTextFeature,
    TextFeatureUnit,
    FeatureList,
    FeatureValidationMode,
    NotFoundStatus,
)

warnings.filterwarnings("ignore")


class AbstractTextFeatureValidator(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


class TextFeatureValidator(AbstractTextFeatureValidator):
    def __init__(
        self,
        client_column: str,
        source_column: str,
        features_list: list[AbstractTextFeature],
        skip_intermediate_validated: bool = True,
    ) -> None:
        self.CLIENT_NAME = client_column
        self.SOURCE_NAME = source_column

        self.skip_intermediate_validated = skip_intermediate_validated
        self.features = FeatureList(features_list)

    def _preproccess(
        self,
        values: list[str],
        feature: AbstractTextFeature,
        unit: TextFeatureUnit,
    ) -> list:
        return [feature(value, unit) for value in values]

    def _data_preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        data[FEATURES.CLIENT_NAME] = "  " + data[self.CLIENT_NAME] + "   "
        data[FEATURES.SOURCE_NAME] = "  " + data[self.SOURCE_NAME] + "   "
        return data

    def _data_clean(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.drop(
            [
                FEATURES.CLIENT_NAME,
                FEATURES.SOURCE_NAME,
                FEATURES.CI,
                FEATURES.SI,
            ],
            axis=1,
        )

        return data

    def _findall(self, cell: str, unit: TextFeatureUnit) -> list[str]:
        output = re.findall(unit.regex, str(cell), re.IGNORECASE)
        return output

    def _feature_search(
        self,
        series: pd.Series,
        feature: AbstractTextFeature,
        unit: TextFeatureUnit,
    ) -> pd.Series:
        series = series.apply(self._findall, args=(unit,))
        series = series.apply(self._preproccess, args=(feature, unit))
        return series

    def _determine_based_intersection(
        self,
        cif: set,
        sif: set,
        val_mode: FeatureValidationMode,
    ) -> int:
        if val_mode is FeatureValidationMode.MODEST:
            based = min(len(cif), len(sif))
        elif val_mode is FeatureValidationMode.CLIENT:
            based = len(cif)
        elif val_mode is FeatureValidationMode.SOURCE:
            based = len(sif)
        else:  # val_mode is FeatureValidationMode.STRICT
            based = max(len(cif), len(sif))
        return based

    def _intermediate_validation_func(
        self,
        row: list[Union[int, set]],
    ) -> list[int]:
        desicion = row[0]
        if desicion == 1:
            cif = row[1]
            sif = row[2]

            not_found_status = NotFoundStatus(
                cif,
                sif,
                self.__not_found_mode,
                self.__feature_name,
            )

            if not_found_status:
                desicion = not_found_status.desicion
                return desicion

            based = self._determine_based_intersection(cif, sif, self.__val_mode)
            intersect = cif.intersection(sif)
            desicion = desicion if len(intersect) == based else 0

        return desicion

    def _intermediate_validation(
        self,
        data: pd.DataFrame,
        feature: AbstractTextFeature,
    ) -> pd.DataFrame:
        cif_massive = map(set, data[FEATURES.CI].to_list())
        sif_massive = map(set, data[FEATURES.SI].to_list())
        intermediate = data[FEATURES.VALIDATED].to_list()

        self.__feature_name = feature.NAME
        self.__val_mode = feature.VALIDATION_MODE
        self.__not_found_mode = feature.NOT_FOUND_MODE

        massive = list(zip(intermediate, cif_massive, sif_massive))
        desicions = list(
            map(self._intermediate_validation_func, massive)
        )  # tqdm(massive)
        data[FEATURES.VALIDATED] = desicions

        return data

    def _extract(self, data: pd.DataFrame) -> pd.DataFrame:
        for feature in self.features:
            feature: AbstractTextFeature

            data[FEATURES.CI] = [[] for _ in range(len(data))]
            data[FEATURES.SI] = [[] for _ in range(len(data))]

            for unit in feature.units:
                cif = self._feature_search(
                    data[FEATURES.CLIENT_NAME],
                    feature,
                    unit,
                )

                sif = self._feature_search(
                    data[FEATURES.SOURCE_NAME],
                    feature,
                    unit,
                )

                data[FEATURES.CI] += cif
                data[FEATURES.SI] += sif

            data[FEATURES.CLIENT] += data[FEATURES.CI]
            data[FEATURES.SOURCE] += data[FEATURES.SI]

            data = self._intermediate_validation(data, feature)
        return data

    def validate(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        data[FEATURES.VALIDATED] = 1
        data = self._data_preprocess(data)

        data.loc[:, FEATURES.CLIENT] = [[] for _ in range(len(data))]
        data.loc[:, FEATURES.SOURCE] = [[] for _ in range(len(data))]

        data = self._extract(data)
        data = self._data_clean(data)

        print("DONE")

        return data


if __name__ == "__main__":
    df = pd.DataFrame()

    df["Название клиент"] = [
        "Сок 1л",
        "Томаты 1кг",
        "Консервы 10шт",
        "Сок 1л",
        "Томаты 1кг",
        "Консервы 10шт",
        "Пальто синее",
        "Пальто зеленое",
        "Таблетка 10мг/мл",
        "Таблетка 10мг/мл",
    ]
    df["Название сайт"] = [
        "Сок 1000мл",
        "Томаты 1000г",
        "Консервы №10",
        "Сок 990мл",
        "Томаты 0.9кг",
        "Консервы 9шт",
        "Пальто синее",
        "Пальто красное",
        "Таблетка 10мг/мл",
        "Таблетка 10г/мл",
    ]

    config = {}
    features = FeatureGenerator().generate(config)

    validator = TextFeatureValidator(
        "Название клиент",
        "Название сайт",
        features_list=features,
    )
    df = validator.validate(df)

    print(df)
