import pandas as pd
import re


class LanguageType(object):
    RUS = "russian"
    ENG = "english"


class Language(object):
    eng = "a-z"
    rus = "а-яё"
    numbers = "0-9"

    def __init__(
        self,
        language: LanguageType,
    ) -> None:
        self.language = language

    def _get_letters(self):
        if self.language is LanguageType.ENG:
            letters = self.eng
        elif self.language is LanguageType.RUS:
            letters = self.rus
        else:
            raise NotImplementedError

        return letters

    def get_letters(self):
        return self._get_letters()

    def get_letters_and_symbols(self, symbols: str = "") -> tuple[str]:
        letters = self.get_letters()

        up = letters.upper() + symbols
        low = letters.lower() + symbols
        both = letters.upper() + letters.lower() + symbols
        return up, low, both

    def get_letters_and_numbers(self) -> tuple[str]:
        letters = self.get_letters()

        up = letters.upper() + self.numbers
        low = letters.lower() + self.numbers
        both = letters.upper() + letters.lower() + self.numbers
        return up, low, both

    def get_letters_numbers_and_symbols(self, symbols: str = "") -> tuple[str]:
        letters = self.get_letters()

        up = letters.upper() + self.numbers + symbols
        low = letters.lower() + self.numbers + symbols
        both = letters.upper() + letters.lower() + self.numbers + symbols
        return up, low, both


class Languages(Language):
    def __init__(self, languages: list[LanguageType]) -> None:
        self.languages = languages

    def _get_letters(self, language):
        if language == "english":
            letters = self.eng
        elif language == "russian":
            letters = self.rus
        return letters

    def get_letters(self):
        letters = "".join([self._get_letters(lang) for lang in self.languages])
        return letters


class LanguageRules(object):
    """
    Rules of words extracting:
    - language - extracting language
    - rule_name - name of rule (will be used as returning column name)
    - startUpper - word should start with capital letter
    - onlyUpper - word should contains only capital letters
    - check_letters - check if word contains letters (wouldn't extract only numbers)
    - symbols - extra symbols which the word can contains
    - word_boundary - boundary of the word \\b (left and right)
    - custom_boundary - custom boundary of the word (left and right)
    - with_numbers - allow extract word which contains numbers
    - min_lenght - min lenght of the word (included)
    - max_words - max count of extracted words (included)
    - stemming - apply stemming to extracted words
    - join_words - apply words joining
    - joiner - symbol for words joining
    """

    def __init__(
        self,
        language: LanguageType = LanguageType.ENG,
        rule_name: str = "",
        startUpper: bool = False,
        onlyUpper: bool = False,
        check_letters: bool = False,
        symbols: str = "",
        word_boundary: bool = False,
        custom_boundary: str = "",
        with_numbers: bool = False,
        min_lenght: int = 0,
        max_words: int = 0,
        stemming: bool = False,
        join_words: bool = True,
        joiner: str = "|",
    ) -> None:
        self.language_name = language
        self.rule_name = rule_name if rule_name else self.language_name
        self.language = Language(language)

        self.startUpper = startUpper
        self.onlyUpper = onlyUpper
        self.symbols = symbols
        self.check_letters = check_letters

        self.word_boundary = word_boundary
        self.custom_boundary = custom_boundary
        self.with_numbers = with_numbers

        self.min_length = min_lenght
        self.max_words = max_words

        self.stemming = stemming
        self.join_words = join_words
        self.joiner = joiner


def _extractWords(
    series: pd.Series,
    rx: str,
    letters: str = "",
    check_letters: bool = False,
) -> pd.Series:
    series = series.astype(str)
    words = series.str.findall(rx)
    if check_letters:
        words = words.apply(
            lambda _words: [
                word for word in _words if re.search(f"[{letters}]", word.lower())
            ]
        )
    return words


def _select_mode(rules: LanguageRules) -> str:
    """Return regex exactly with LanguageRules"""

    # We should do like this because the word can't start with symbols
    if rules.with_numbers:
        upCl, lowCl, bothCl = rules.language.get_letters_and_numbers()
        up, low, both = rules.language.get_letters_numbers_and_symbols(
            symbols=rules.symbols
        )
    else:
        upCl, lowCl, bothCl = rules.language.get_letters_and_symbols(
            symbols=""
        )  # clear version
        up, low, both = rules.language.get_letters_and_symbols(
            symbols=rules.symbols
        )  # with symbols

    if rules.onlyUpper:
        rx = rf"[{upCl}][{up}]*"
    else:
        rx = rf"[{upCl}][{both}]*" if rules.startUpper else rf"[{bothCl}][{both}]*"

    return rx


def _add_boundary(rx: str, rules: LanguageRules) -> str:
    if rules.word_boundary or rules.custom_boundary:
        if not rules.custom_boundary:
            rx = r"(?:\b)" + rf"({rx})" + r"(?:\b)"
        else:
            rx = (
                rf"(?:{rules.custom_boundary})"
                + rf"({rx})"
                + rf"(?:{rules.custom_boundary})"
            )
    return rx


def extractWords(
    series: pd.Series,
    rules: LanguageRules,
) -> pd.Series:
    letters = rules.language.get_letters()
    rx = _select_mode(rules)
    rx = _add_boundary(rx, rules)

    words = _extractWords(series, rx, letters, rules.check_letters)
    return words


def _deleteWords(
    series: pd.Series,
    rx: str,
) -> pd.Series:
    series = series.astype(str)
    words = series.str.replace(rx, " ", regex=True)
    return words


def deleteWords(
    series: pd.Series,
    rules: LanguageRules,
) -> pd.Series:
    rx = _select_mode(rules)
    rx = _add_boundary(rx, rules)

    words = _deleteWords(series, rx)
    return words


def extractWordsWithMultipleLangsLetters(
    series: pd.Series,
    rules: list[LanguageRules],
) -> pd.Series:
    def _concat_langs(rules):
        regex = "("
        for index in range(len(rules)):
            rule = rules[index]
            if index < len(rules) - 1:
                _rx = _select_mode(rule)
                _rx = _add_boundary(_rx, rule)
                _rx += "|"
            else:
                _rx = _select_mode(rule)
                _rx = _add_boundary(_rx, rule)
                _rx += ")"
            regex += _rx
        return regex

    def unpack(word):
        if isinstance(word, tuple):
            word = word[0]
        return word

    rx = _concat_langs(rules)

    words = _extractWords(series, rx)
    words = words.apply(lambda _words: [unpack(word) for word in _words])

    return words
