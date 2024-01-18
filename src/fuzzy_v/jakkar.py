import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
import multiprocessing


SRC_DIR = Path(__file__).parent.parent
PROJECT_DIR = SRC_DIR.parent

sys.path.append(str(PROJECT_DIR))

from src.notation import JAKKAR, DATA
from src.fuzzy_v.preprocessing import Preprocessor
from src.fuzzy_v.fuzzy_search import FuzzySearch
from src.fuzzy_v.ratio import RateCounter, MarksCounter, MarksMode, RateFunction
from src.fuzzy_v.tokenization import (
    BasicTokenizer,
    TokenTransformer,
    RegexTokenizer,
    RegexCustomWeights,
    LanguageType,
)
from config.fuzzy_config.config_parser import (
    CONFIG,
    REGEX_WEIGHTS,
    LANGUAGE_WEIGHTS,
    RATIO,
)


class FuzzyJakkarValidator(object):
    def __init__(
        self,
        tokenizer: BasicTokenizer,
        preprocessor: Preprocessor,
        fuzzy: FuzzySearch,
        rate_counter: RateCounter,
        marks_counter: MarksCounter,
        debug: bool = False,
        validation_treshold: float = 0.5,
    ) -> None:
        if validation_treshold < 0 or validation_treshold > 1:
            raise ValueError("Validation treshold should be in range 0 - 1")

        self.tokenizer = tokenizer
        self.preproc = preprocessor
        self.fuzzy = fuzzy
        self.rate_counter = rate_counter
        self.marks_counter = marks_counter
        self.debug = debug
        self.validation_treshold = validation_treshold

        self.symbols_to_del = r"'\"/"

    def _progress_ind(self, indicator: str) -> None:
        match indicator:
            case "start":
                print("Start Fuzzy Jakkar matching")
            case "client_tokens":
                print("Tokenize client tokens")
            case "source_tokens":
                print("Tokenize site tokens")
            case "make_filter":
                print("Make tokens filter")
            case "make_set":
                print("Make set of tokens")
            case "make_fuzzy":
                print("Start fuzzy search")
            case "make_ratio":
                print("Start count match ratio")
            case "end":
                print("End the validation process: save output")
            case "delete_rx":
                print("Deleting elements from rows by regex")

    def _delete_symbols(self, series: pd.Series):
        symbols_to_del = "|".join(list(self.symbols_to_del))
        series = series.str.replace(symbols_to_del, "", regex=True)
        return series

    def _create_working_rows(
        self,
        data: pd.DataFrame,
        client_column: str,
        source_column: str,
    ) -> pd.DataFrame:
        data[JAKKAR.CLIENT] = self._delete_symbols(data[client_column])
        data[JAKKAR.SOURCE] = self._delete_symbols(data[source_column])
        return data

    def _delete_working_rows(self, data: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("end")
        data.drop(
            [
                JAKKAR.CLIENT,
                JAKKAR.SOURCE,
                JAKKAR.CLIENT_TOKENS_COUNT,
                JAKKAR.SOURCE_TOKENS_COUNT,
            ],
            axis=1,
            inplace=True,
            errors="ignore",
        )

        if not self.debug:
            data.drop(
                [JAKKAR.CLIENT_TOKENS, JAKKAR.SOURCE_TOKENS],
                axis=1,
                inplace=True,
            )
        return data

    def _save_ratio(self) -> None:
        pd.Series(data=self.ratio).to_excel(JAKKAR.RATIO_PATH)

    def _process_tokenization(self, data: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("client_tokens")
        data = self.tokenizer.tokenize(data, JAKKAR.CLIENT, JAKKAR.CLIENT_TOKENS)

        self._progress_ind("source_tokens")
        data = self.tokenizer.tokenize(data, JAKKAR.SOURCE, JAKKAR.SOURCE_TOKENS)

        return data

    def _make_tokens_set(self, data: pd.DataFrame) -> pd.DataFrame:
        data[JAKKAR.CLIENT_TOKENS] = data[JAKKAR.CLIENT_TOKENS].apply(set)
        data[JAKKAR.SOURCE_TOKENS] = data[JAKKAR.SOURCE_TOKENS].apply(set)
        return data

    def _process_preprocessing(self, validation: pd.DataFrame) -> pd.DataFrame:
        validation[JAKKAR.CLIENT_TOKENS] = self.preproc.preprocess(
            validation[JAKKAR.CLIENT_TOKENS]
        )
        validation[JAKKAR.SOURCE_TOKENS] = self.preproc.preprocess(
            validation[JAKKAR.SOURCE_TOKENS]
        )
        return validation

    def _process_fuzzy(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        self._progress_ind("make_fuzzy")
        data = self.fuzzy.search(
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
        )
        return data

    def _process_ratio(self, data: pd.DataFrame) -> pd.DataFrame:
        self._progress_ind("make_ratio")
        ratio = self.rate_counter.count_ratio(
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
        )
        return ratio

    def _process_marks_count(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        return self.marks_counter.count_marks(
            self.ratio,
            data,
            JAKKAR.CLIENT_TOKENS,
            JAKKAR.SOURCE_TOKENS,
        )

    def _process_tokens_count(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        data[JAKKAR.CLIENT_TOKENS_COUNT] = data[JAKKAR.CLIENT_TOKENS].apply(
            lambda x: len(x)
        )
        data[JAKKAR.SOURCE_TOKENS_COUNT] = data[JAKKAR.SOURCE_TOKENS].apply(
            lambda x: len(x)
        )
        return data

    def validate(
        self,
        data: pd.DataFrame,
        client_column: str,
        source_column: str,
    ) -> pd.DataFrame:
        data = self._create_working_rows(data, client_column, source_column)

        data = self._process_tokenization(data)
        data = self._process_preprocessing(data)

        # очистка токенов-символов по типу (, ), \, . и т.д.
        # актуально для word_tokenizer
        data = self._process_fuzzy(data)
        self.ratio = self._process_ratio(data)

        data = self._make_tokens_set(data)
        data = self._process_tokens_count(data)
        data = self._process_marks_count(data)

        if self.debug:
            self._save_ratio()

        data[JAKKAR.VALIDATED] = np.where(
            data[self.marks_counter.validation_column] >= self.validation_treshold,
            1,
            0,
        )

        data = self._delete_working_rows(data)
        return data


def setup_jakkar_validator(
    config: dict,
    fuzzy_threshold: float,
    validation_threshold: float,
) -> FuzzyJakkarValidator:
    regex_weights = RegexCustomWeights(
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.CAPS],
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.CAPITAL],
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.LOW],
        config[CONFIG.REGEX_WEIGHTS][REGEX_WEIGHTS.OTHER],
    )

    tokenizer = RegexTokenizer(
        {
            LanguageType.RUS: config[CONFIG.LANGUAGE_WEIGHTS][LANGUAGE_WEIGHTS.RUS],
            LanguageType.ENG: config[CONFIG.LANGUAGE_WEIGHTS][LANGUAGE_WEIGHTS.ENG],
        },
        weights_rules=regex_weights,
    )

    preprocessor = Preprocessor(config[CONFIG.WORD_MIN_LEN])
    transformer = TokenTransformer()
    rate_counter = RateCounter(
        config[CONFIG.RATIO][RATIO.MIN_RATIO],
        config[CONFIG.RATIO][RATIO.MAX_RATIO],
        config[CONFIG.RATIO][RATIO.MIN_APPEARANCE],
        config[CONFIG.RATIO][RATIO.MIN_APPEARANCE_PENALTY],
        RateFunction.map(config[CONFIG.RATIO][RATIO.RATE_FUNC]),
    )
    fuzzy = FuzzySearch(fuzzy_threshold, transformer=transformer)
    marks_counter = MarksCounter(MarksMode.MULTIPLE)

    fuzzy_validator = FuzzyJakkarValidator(
        tokenizer=tokenizer,
        preprocessor=preprocessor,
        fuzzy=fuzzy,
        rate_counter=rate_counter,
        marks_counter=marks_counter,
        validation_treshold=validation_threshold,
    )
    return fuzzy_validator


def read_config(path: str | Path) -> dict:
    with open(path, "rb") as file:
        data = json.loads(file.read())
    return data


if __name__ == "__main__":
    data = pd.read_excel("/home/mainus/Projects/ProductMatcher/FarmaImpex2.xlsx")

    config = read_config(
        "/home/mainus/Projects/ProductMatcher/config/fuzzy_config/setups/main.json"
    )

    validator = setup_jakkar_validator(config, 0.75, 0.5)
    data: pd.DataFrame = validator.validate(
        data,
        "Строка валидации",
        "Наименование товара клиента",
    )
    data.to_excel("jakkar_check.xlsx")
