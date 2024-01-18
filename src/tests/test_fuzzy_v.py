import sys
import pytest
import regex as re
import pandas as pd
from pathlib import Path


PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from src.fuzzy_v.jakkar import setup_jakkar_validator, FuzzyJakkarValidator
from src.notation import JAKKAR
from src.tests.common_test import (
    FUZZY_CONFIG,
    CLIENT_PRODUCT,
    IS_EQUAL,
    DEBUG,
    SOURCE_PRODUCT,
    FuzzyDataSet,
)

MAX_ERROR_RATE = 0.97


class BaseTestFuzzyV(object):
    debug = False

    def validator(
        self,
        config: dict = FUZZY_CONFIG,
        fuzzy_threshold: float = 0.75,
        validation_threshold: float = 0.5,
    ) -> FuzzyJakkarValidator:
        validator = setup_jakkar_validator(
            config=config,
            fuzzy_threshold=fuzzy_threshold,
            validation_threshold=validation_threshold,
        )
        return validator

    def checkout(self, data: pd.DataFrame) -> bool:
        check_sum = (data[IS_EQUAL] == data[JAKKAR.VALIDATED]).sum()
        error_rate = check_sum / len(data)
        check = error_rate > MAX_ERROR_RATE

        if self.debug and not check:
            data[DEBUG] = data[IS_EQUAL] - data[JAKKAR.VALIDATED]
            data = data[data[DEBUG] != 0]
            data.to_excel("debug.xlsx", index=False)

        return check

    def run_validation_test(
        self,
        data: pd.DataFrame,
        validator: FuzzyJakkarValidator,
    ):
        data = validator.validate(data, CLIENT_PRODUCT, SOURCE_PRODUCT)
        assert self.checkout(data)


class TestFuzzyVGenerics(BaseTestFuzzyV):
    def test_generics_fuzzy_validation(self):
        self.run_validation_test(
            FuzzyDataSet.small(),
            self.validator(
                fuzzy_threshold=0.75,
                validation_threshold=0.5,
            ),
        )


class FuzzyVGenericsTestsDebug(TestFuzzyVGenerics):
    def __init__(self) -> None:
        super().__init__()
        self.debug = True


if __name__ == "__main__":
    test = FuzzyVGenericsTestsDebug()
    test.test_generics_fuzzy_validation()
