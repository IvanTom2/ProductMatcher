import sys
import pytest
import regex as re
import pandas as pd
from pathlib import Path


PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from common_test import (
    NumericDataSet,
    StringDataSet,
    CLIENT_PRODUCT,
    SOURCE_PRODUCT,
    IS_EQUAL,
    MEASURES_CONFIG,
)
from src.feature_v.feature_validator import (
    TextFeatureValidator,
    FeatureGenerator,
    FEATURES,
)


class BaseTestFeatureV(object):
    debug = False

    def validator(self):
        features = FeatureGenerator().generate(MEASURES_CONFIG)
        return TextFeatureValidator(
            CLIENT_PRODUCT,
            SOURCE_PRODUCT,
            features,
        )

    def checkout(self, data: pd.DataFrame) -> bool:
        check = (data[IS_EQUAL] != data[FEATURES.VALIDATED]).sum() == 0
        if self.debug:
            data["DEBUG"] = data[IS_EQUAL] - data[FEATURES.VALIDATED]
            data = data[data["DEBUG"] != 0]
            data.to_excel("debug.xlsx", index=False)

        return check

    def run_generic(
        self,
        data: pd.DataFrame,
        validator: TextFeatureValidator,
    ):
        data = validator.validate(data)
        assert self.checkout(data)


class TestFeatureVGenerics(BaseTestFeatureV):
    def __init__(self, debug: bool = False) -> None:
        super().__init__()
        self.debug = debug

    def test_full_validation(self):
        data = pd.concat(
            [
                NumericDataSet.all(),
                StringDataSet.all(),
            ]
        )

        self.run_generic(
            data,
            self.validator(),
        )


if __name__ == "__main__":
    test = TestFeatureVGenerics()
    test.test_full_validation()
