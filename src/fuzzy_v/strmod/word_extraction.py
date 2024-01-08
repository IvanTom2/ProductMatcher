import pandas as pd

from src.fuzzy_v.strmod.word_extraction_funcs import (
    LanguageRules,
    LanguageType,
    extractWords,
    deleteWords,
    extractWordsWithMultipleLangsLetters,
)
from src.fuzzy_v.strmod.common import (
    Extractor,
    wordsCleaner,
    wordsFilter,
    wordsStemming,
    wordsJoin,
)


class WordsExtractor(Extractor):
    def __init__(
        self,
        rules: LanguageRules,
        expand_spaces: bool = False,
        del_founded: bool = False,
    ):
        self.rules = rules
        self.expand_spaces = expand_spaces
        self.del_founded = del_founded

    def _preprocess(
        self,
        words: pd.Series,
    ) -> pd.Series:
        words = wordsCleaner(words)
        words = wordsFilter(
            words,
            min_length=self.rules.min_length,
            max_words=self.rules.max_words,
        )

        if self.rules.stemming:
            words = wordsStemming(
                words,
                language=self.rules.language_name,
            )

        if self.rules.join_words:
            words = wordsJoin(words, joiner=self.rules.joiner)
        return words

    def _extract(self, data: pd.DataFrame, col: str) -> pd.DataFrame:
        words = extractWords(data["_rows"], self.rules)
        if self.del_founded:
            data["_rows"] = deleteWords(data["_rows"], self.rules)
            data[col] = data["_rows"].str.replace(r"[ ]+", r" ", regex=True)

        words = self._preprocess(words)
        return words

    def extract(
        self,
        data: pd.DataFrame,
        col: str,
        return_mode: str = "dataframe",
    ) -> pd.DataFrame:
        if return_mode not in ["series", "dataframe"]:
            raise ValueError("return_mode should be 'series' or 'dataframe'.")

        data["_rows"] = data[col]
        if self.expand_spaces:
            data["_rows"] = "  " + data["_rows"] + "  "
            data["_rows"] = data["_rows"].str.replace(" ", " " * 3)

        words = self._extract(data, col)
        data.drop("_rows", axis=1, inplace=True)
        if return_mode == "series":
            return words
        elif return_mode == "dataframe":
            data[self.rules.rule_name] = words
            return data


class StraightWordExtractor(Extractor):
    def __init__(
        self,
        rules: list[LanguageRules],
        straight: bool = False,
        main_rule: int = 0,
        expand_spaces: bool = False,
        del_founded: bool = False,
    ):
        if straight and not isinstance(rules, list):
            rules = [rules]

        self.rules = rules
        self.straight = straight
        self.main_rule = main_rule
        self.expand_spaces = expand_spaces
        self.del_founded = del_founded

    def _preprocess(
        self,
        words: pd.Series,
        rules: LanguageRules,
    ) -> pd.Series:
        words = wordsCleaner(words)
        words = wordsFilter(
            words,
            min_length=rules.min_length,
            max_words=rules.max_words,
        )

        if rules.stemming:
            words = wordsStemming(
                words,
                language=rules.language_name,
            )

        if rules.join_words:
            words = wordsJoin(words, joiner=rules.joiner)
        return words

    def _straight(self, data: pd.DataFrame, newcol: str):
        words = extractWordsWithMultipleLangsLetters(data["_rows"], self.rules)
        data[newcol] = self._preprocess(words, self.rules[self.main_rule])

        return data

    def extract(
        self,
        data: pd.DataFrame,
        col: str,
        newcol: str = "straight",
    ) -> pd.DataFrame:
        data["_rows"] = data[col]
        if self.expand_spaces:
            data["_rows"] = "  " + data["_rows"] + "  "
            data["_rows"] = data["_rows"].str.replace(" ", " " * 3)

        data = self._straight(data, newcol)

        data.drop("_rows", axis=1, inplace=True)
        return data


if __name__ == "__main__":
    data = pd.DataFrame(
        data=["Привет Мир! ЖОПА1234-1234 Ваня228 Ajax Ajax17"], columns=["Название"]
    )

    ru = LanguageRules(
        LanguageType.RUS,
        rule_name="жопа",
        word_boundary=True,
        with_numbers=True,
        startUpper=True,
        check_letters=True,
    )

    extractor = WordsExtractor(ru, True, True)

    data = extractor.extract(data, "Название")
    print(data)
