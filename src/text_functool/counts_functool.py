import re
import pandas as pd


class CountFuncTool(object):
    def extract(
        self,
        series: pd.Series,
        counts: str,
    ) -> pd.Series:
        rx = r"(\d+)\s*?" + counts

        countsValues = series.str.findall(rx, flags=re.IGNORECASE)
        countsValues = countsValues.apply(
            lambda inp: [countValue for countValue in inp if str(countValue) != "1"]
        )
        return countsValues

    def create_rx(
        self,
        countsValues: pd.Series,
        counts: str,
    ) -> pd.Series:
        countsRX = countsValues.str[:1].str.join("").astype(str)
        countsRX = countsRX.where(
            countsRX == "",
            r"(?=.*(\D" + countsRX + r"\s*?" + counts + "))",
        )

        return countsRX

    def create_exclude_rx(
        self,
        countsValues: pd.Series,
        counts: str,
    ) -> pd.Series:
        def fullmatch(cell):
            if re.fullmatch(r"\d+", cell):
                return False
            return True

        countsRX = countsValues.str[:1].str.join("").astype(str)
        countsRX = countsRX.where(
            countsRX != "",
            r"(?!.*((?:[0-9][0-9]\d*?|[2-9]\d*?)\s*?" + counts + "))",
        )

        countsRX = countsRX.where(countsRX.apply(fullmatch), "")
        return countsRX


class CountNumeroFuncTool(object):
    def excract(
        self,
        series: pd.Series,
        counts: str,
    ) -> pd.Series:
        rx = counts + r"\s*?(\d+)"

        countsValues = series.str.findall(rx, flags=re.IGNORECASE)
        countsValues = countsValues.apply(
            lambda inp: [countValue for countValue in inp if str(countValue) != "1"]
        )
        return countsValues

    def create_rx(
        self,
        countsValues: pd.Series,
        default_counts: str,
        numero_counts: str,
    ) -> pd.Series:
        countsRX = countsValues.str[:1].str.join("").astype(str)
        countsRX = countsRX.where(
            countsRX == "",
            r"(?=.*(\D"
            + countsRX
            + r"\s*?"
            + default_counts
            + "|"
            + numero_counts
            + r"\s*?"
            + countsRX
            + r"\D))",
        )

        return countsRX

    def create_exclude_rx(
        self,
        countsValues: pd.Series,
        default_counts: str,
        numero_counts: str,
    ) -> pd.Series:
        def fullmatch(cell):
            if re.fullmatch(r"\d+", cell):
                return False
            return True

        countsRX = countsValues.str[:1].str.join("").astype(str)
        countsRX = countsRX.where(
            countsRX != "",
            r"(?!.*((?:[0-9][0-9]\d*?|[2-9]\d*?)\s*?"
            + default_counts
            + "|"
            + numero_counts
            + r"(?:[0-9][0-9]\d*?|[2-9]\d*?)"
            + "))",
        )

        countsRX = countsRX.where(countsRX.apply(fullmatch), "")
        return countsRX
