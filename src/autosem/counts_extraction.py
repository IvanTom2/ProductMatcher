import warnings
import pandas as pd

from common import Extractor
from common import CountFuncTool, CountNumeroFuncTool

warnings.filterwarnings("ignore")


class CountsExtractor(Extractor):
    """
    Extract counts from rows and make regex for them.
    Example: 15 шт -> extract "15"

    - counts - counts feature for extracting through regex.
    - exclude_rx - make special regex for rows where counts weren't extracted.
    """

    def __init__(
        self,
        counts: str = "шт|уп|бан|пач",
        exclude_rx: bool = False,
    ) -> None:
        self.counts = counts
        self.exclude_rx = exclude_rx

        self.tool = CountFuncTool()

    def extract(
        self,
        data: pd.DataFrame,
        col: str = "Название",
    ) -> pd.DataFrame:
        """
        return dataframe with two extra columns:
        1) "Количество"
        2) "Исключающее количество"
        """
        counts = "(?:" + self.counts + ")"
        countsValues = self.tool.extract(data[col], counts)
        countsDefaultRX = self.tool.create_rx(countsValues, counts)

        countsExcludeRX = pd.Series()
        if self.exclude_rx:
            countsExcludeRX = self.tool.create_exclude_rx(countsValues, counts)

        data["Количество"] = countsDefaultRX
        data["Исключающее количество"] = countsExcludeRX

        return data


class CountsNumeroExtractor(Extractor):
    """
    Extract counts and '№' from rows and make regex for them.
    Example: 15 шт -> extract "15"
    Example: №16 -> extract "16"

    - counts - counts feature for extracting through regex.
    - NO - '№' feature for extracting through regex.
    - exclude_rx - make special regex for rows where counts weren't extracted.
    """

    def __init__(
        self,
        counts: str = "шт|уп",
        numero: str = "№|n",
        exclude_rx: bool = False,
    ) -> None:
        self.counts = counts
        self.numero = numero
        self.exclude_rx = exclude_rx

        self.count_tool = CountFuncTool()
        self.count_numero_tool = CountNumeroFuncTool()

    def extract(
        self,
        data: pd.DataFrame,
        col="Название",
    ) -> pd.DataFrame:
        """
        return dataframe with two extra columns:
        1) "Количество (№)"
        2) "Исключающее количество (№)"
        """

        default_counts = "(?:" + self.counts + ")"
        numero_counts = "(?:" + self.numero + ")"

        default_values = self.count_tool.extract(data[col], default_counts)
        numero_values = self.count_numero_tool.excract(data[col], numero_counts)

        # numero_values in 1-st priority
        countsValues = numero_values + default_values
        countsDefaultRX = self.count_numero_tool.create_rx(
            countsValues,
            default_counts,
            numero_counts,
        )

        countsExcludeRX = pd.Series()
        if self.exclude_rx:
            countsExcludeRX = self.count_numero_tool.create_exclude_rx(
                countsValues,
                default_counts,
                numero_counts,
            )

        data["Количество (№)"] = countsDefaultRX
        data["Исключающее количество (№)"] = countsExcludeRX

        return data


if __name__ == "__main__":
    data = pd.DataFrame()
    data.at[0, "Название"] = "Яблоки 19 штук"
    data.at[1, "Название"] = "Яблоки 10 упаковок"
    data.at[2, "Название"] = "Яблоки 5 штук №228"
    data.at[3, "Название"] = "Яблоки 1 штук"
    data.at[4, "Название"] = "Яблоки №3"

    extractor = CountsNumeroExtractor(exclude_rx=True)
    data = extractor.extract(data, "Название")

    print(data)
