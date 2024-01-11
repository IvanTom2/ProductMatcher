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
    REGEX,
    CHECKOUT,
    IS_EQUAL,
    MEASURES_CONFIG,
    DataTypes,
)
from src.autosem.measures_extraction import MeasureExtractor, MeasuresExtractor


class BaseTestAutosem(object):
    debug = False

    def extractor(self):
        return MeasureExtractor(MEASURES_CONFIG)

    def extract(self, row: pd.Series) -> str:
        search = re.search(row[REGEX], row[SOURCE_PRODUCT], flags=re.IGNORECASE)
        return 1 if search else 0

    def checkout(self, data: pd.DataFrame) -> bool:
        data[SOURCE_PRODUCT] = "   " + data[SOURCE_PRODUCT].astype(str) + "   "

        data[CHECKOUT] = data.apply(self.extract, axis=1)
        check = (data[IS_EQUAL] - data[CHECKOUT]).sum() == 0
        if self.debug:
            data["DEBUG"] = data[IS_EQUAL] - data[CHECKOUT]
            data = data[data["DEBUG"] != 0]
            data.to_excel("debug.xlsx", index=False)

        data.loc[:, SOURCE_PRODUCT] = data[SOURCE_PRODUCT].str.strip()
        return check

    def run_generic(
        self,
        data: pd.DataFrame,
        measure_name: DataTypes,
        extractor: MeasureExtractor,
    ):
        data[REGEX] = extractor.extract(data, CLIENT_PRODUCT, measure_name)
        assert self.checkout(data)


class TestAutosemGenerics(BaseTestAutosem):
    # def __init__(self, debug: bool = False) -> None:
    #     super().__init__()
    #     self.debug = debug

    def test_extract_weight(self):
        self.run_generic(
            NumericDataSet.weight_data(),
            DataTypes.weight,
            self.extractor(),
        )

    def test_extract_volume(self):
        self.run_generic(
            NumericDataSet.volume_data(),
            DataTypes.volume,
            self.extractor(),
        )

    def test_extract_quantity(self):
        self.run_generic(
            NumericDataSet.quantity_data(),
            DataTypes.quantity,
            self.extractor(),
        )

    def test_extract_color(self):
        self.run_generic(
            StringDataSet.color_data(),
            DataTypes.color,
            self.extractor(),
        )


if __name__ == "__main__":
    test_debug = TestAutosemGenerics(True)
    test_debug.test_extract_quantity()
