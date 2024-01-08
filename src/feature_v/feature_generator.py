import json
import sys
import regex as re
from decimal import Decimal
from pathlib import Path

SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent

sys.path.append(str(PROJECT_DIR))
from config.measures_config.config_parser import (
    CONFIG,
    MEASURE,
    DATA,
    FEATURE,
    TEXT_FEATURES_CONF,
)
from src.functool.measures_functool import (
    NumericFeature,
    StringFeature,
    MeasureFeature,
)
from src.feature_v.feature_functool import (
    AbstractTextFeature,
    TextFeatureUnit,
    FeatureValidationMode,
    FeatureNotFoundMode,
)
from src.feature_v.complex_features import COMPLEX_MAP


def NumericTextFeatureFabrique(name: str) -> AbstractTextFeature:
    class NumericTextFeature(AbstractTextFeature):
        def __init__(
            self,
            value: str,
            unit: TextFeatureUnit,
        ) -> None:
            self.original_value = value
            self.standard_value = self._standartization(value, unit)

        def _standartization(self, value: str, unit: TextFeatureUnit):
            num_value = re.search(r"\d+[.,]?\d*", value)[0]
            num_value = num_value.replace(",", ".")
            num_value = Decimal(num_value)

            kf = unit.weight
            num_value = num_value * kf

            return num_value

        def __eq__(self, other: AbstractTextFeature) -> bool:
            if isinstance(other, self.__class__):
                if self.standard_value == other.standard_value:
                    return True
            return False

        def __hash__(self) -> int:
            return hash(self.standard_value)

        def __repr__(self) -> str:
            return rf"{self.NAME} = {self.standard_value}"

        def __str__(self) -> str:
            return rf"{self.NAME} = {self.standard_value}"

        @classmethod
        @property
        def units(self):
            return self.UNITS

    return NumericTextFeature


def StringTextFeatureFabrique(name: str) -> AbstractTextFeature:
    class StringTextFeature(AbstractTextFeature):
        def __init__(
            self,
            value: str,
            unit: TextFeatureUnit,
        ) -> None:
            self.original_value = value
            self.standard_value = self._standartization(unit)

        def _standartization(self, unit: TextFeatureUnit) -> str:
            return unit.name

        def __eq__(self, other: AbstractTextFeature) -> bool:
            if isinstance(other, self.__class__):
                if self.standard_value == other.standard_value:
                    return True
            return False

        def __hash__(self) -> int:
            return hash(self.standard_value)

        def __repr__(self) -> str:
            return rf"{self.NAME} = {self.standard_value}"

        def __str__(self) -> str:
            return rf"{self.NAME} = {self.standard_value}"

        @classmethod
        @property
        def units(self):
            return self.UNITS

    return StringTextFeature


def ComplexTextFeatureFabrique(name: str) -> AbstractTextFeature:
    return COMPLEX_MAP[name]


class FeatureCreatorTool(object):
    @classmethod
    def _create(
        self,
        fabrique: callable,
        name: str,
        units: list[TextFeatureUnit],
        validation_mode: str,
        not_found_mode: str,
        priority: int,
    ) -> AbstractTextFeature:
        feature: AbstractTextFeature = fabrique(name)
        feature.NAME = name
        feature.UNITS = units

        feature.VALIDATION_MODE = FeatureValidationMode.checkout(validation_mode)
        feature.NOT_FOUND_MODE = FeatureNotFoundMode.checkout(not_found_mode)
        feature.PRIORITY = priority

        return feature

    @classmethod
    def _create_numeric(
        self,
        name: str,
        units: list[TextFeatureUnit],
        validation_mode: str,
        not_found_mode: str,
        priority: int,
    ):
        return self._create(
            NumericTextFeatureFabrique,
            name,
            units,
            validation_mode,
            not_found_mode,
            priority,
        )

    @classmethod
    def _create_string(
        self,
        name: str,
        units: list[TextFeatureUnit],
        validation_mode: str,
        not_found_mode: str,
        priority: int,
    ):
        return self._create(
            StringTextFeatureFabrique,
            name,
            units,
            validation_mode,
            not_found_mode,
            priority,
        )

    @classmethod
    def _create_complex(
        self,
        name: str,
        units: list[TextFeatureUnit],
        validation_mode: str,
        not_found_mode: str,
        priority: int,
    ):
        return self._create(
            ComplexTextFeatureFabrique,
            name,
            units,
            validation_mode,
            not_found_mode,
            priority,
        )


class FeatureGenerator(object):
    __type_mapper = {
        CONFIG.NUMERIC_MEASURES: NumericFeature,
        CONFIG.STRING_MEASURES: StringFeature,
        CONFIG.COMPLEX_MEASURES: None,
    }

    __func_mapper = {
        CONFIG.NUMERIC_MEASURES: FeatureCreatorTool._create_numeric,
        CONFIG.STRING_MEASURES: FeatureCreatorTool._create_string,
        CONFIG.COMPLEX_MEASURES: FeatureCreatorTool._create_complex,
    }

    def _generate_complex(self, config: dict) -> list[AbstractTextFeature]:
        complex_features = []
        creator_func = self.__func_mapper[CONFIG.COMPLEX_MEASURES]
        if config[CONFIG.COMPLEX_MEASURES][CONFIG.USE_IT]:
            measure_records = config[CONFIG.COMPLEX_MEASURES][CONFIG.MEASURES]
            for measure_record in measure_records:
                feature_conf = measure_record[MEASURE.TEXT_FEATURES]
                feature = creator_func(
                    measure_record[MEASURE.NAME],
                    [],
                    feature_conf[TEXT_FEATURES_CONF.VALIDATION_MODE],
                    feature_conf[TEXT_FEATURES_CONF.NOT_FOUND_MODE],
                    feature_conf[TEXT_FEATURES_CONF.PRIORITY],
                )
                complex_features.append(feature)

        return complex_features

    def _generate_default(self, config: dict) -> list[AbstractTextFeature]:
        default_features = []
        for MEASURE_TYPE in CONFIG.MEASURE_TYPES:
            type: MeasureFeature = self.__type_mapper[MEASURE_TYPE]
            creator_func = self.__func_mapper[MEASURE_TYPE]

            if config[MEASURE_TYPE][CONFIG.USE_IT]:
                data = config[MEASURE_TYPE]

                for measure_record in data[CONFIG.MEASURES]:
                    if MEASURE.NAME in measure_record:
                        measure_data = measure_record[MEASURE.DATA]
                        feature_conf = measure_record[MEASURE.TEXT_FEATURES]

                        if feature_conf[TEXT_FEATURES_CONF.USE_IT]:
                            units = []
                            for feature_data in measure_data[DATA.FEATURES]:
                                _feature: MeasureFeature = type(
                                    feature_data,
                                    measure_data[DATA.COMMON_PREFIX],
                                    measure_data[DATA.COMMON_POSTFIX],
                                    measure_data[DATA.COMMON_MAX_COUNT],
                                    measure_data[DATA.SPECIAL_VALUE_SEARCH],
                                )

                                regex = _feature.get_search_regex()
                                unit = TextFeatureUnit(
                                    feature_data[FEATURE.NAME],
                                    regex,
                                    feature_data[FEATURE.RWEIGHT],
                                )
                                units.append(unit)

                            feature = creator_func(
                                measure_record[MEASURE.NAME],
                                units,
                                feature_conf[TEXT_FEATURES_CONF.VALIDATION_MODE],
                                feature_conf[TEXT_FEATURES_CONF.NOT_FOUND_MODE],
                                feature_conf[TEXT_FEATURES_CONF.PRIORITY],
                            )
                            default_features.append(feature)

        return default_features

    def generate(self, config: dict) -> list[AbstractTextFeature]:
        """Parse config and create dict with Measure objects
        accorging to parsed rules"""

        features_list = []
        features_list.extend(self._generate_default(config))
        features_list.extend(self._generate_complex(config))

        return features_list
