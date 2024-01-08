import pandas as pd
import re
import nltk
from abc import ABC, abstractmethod


class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass


def choose_stemmer():
    # stemmer = nltk.stem.PorterStemmer
    stemmer = nltk.stem.SnowballStemmer
    # stemmer = nltk.stem.Cistem
    # stemmer = nltk.stem.LancasterStemmer
    # stemmer = nltk.stem.RegexpStemmer
    # stemmer = nltk.stem.StemmerI
    # stemmer = nltk.stem.WordNetLemmatizer
    # stemmer = nltk.stem.RSLPStemmer  # Portuguese
    # stemmer = nltk.stem.ISRIStemmer  # Arabic
    # stemmer = nltk.stem.ARLSTem  # Arabic
    # stemmer = nltk.stem.ARLSTem2  # Arabic

    return stemmer


def wordsJoin(words: pd.Series, joiner="|") -> pd.Series:
    """Return series of joined by joiner words"""
    words = words.apply(lambda _words: joiner.join(_words))
    return words


def wordsStemming(words: pd.Series, language="english") -> pd.Series:
    # TODO: choose stemmer
    # stemmer = choose_stemmer()
    # current stemmer = snowball

    stemmer = nltk.stem.SnowballStemmer(language)
    words = words.apply(lambda _words: [stemmer.stem(word) for word in _words])
    return words


def wordsCleaner(words: pd.Series) -> pd.Series:
    words = words.apply(lambda _words: [w.strip() for w in _words])
    return words


def wordsFilter(
    words: pd.Series,
    min_length: int = 0,
    max_words: int = 0,
) -> pd.Series:
    """
    min_lenght: min lenght of word (included)
    max_words: maximum count of extracted words (included)
    """

    def _filter(_words: list) -> list:
        output = list(filter(lambda x: len(x) >= min_length, _words))
        output = output if not max_words else output[:max_words]
        return output

    words = words.apply(_filter)
    return words


def parse_rx(
    data: pd.DataFrame,
    extract_col: str = "regex",
    new_col_name: str = "rx_to_del",
) -> pd.DataFrame:
    data[new_col_name] = data[extract_col].str.findall(r"\(\?\=\.\*.*?\)\)+")
    data[new_col_name] = data[new_col_name].apply(
        lambda rxs: [re.sub(r"\(\?\=\.\*\(", "", rx) for rx in rxs]
    )
    data[new_col_name] = data[new_col_name].apply(
        lambda rxs: [re.sub(r"\)\)$", "", rx) for rx in rxs]
    )
    return data


def del_rx(data: pd.DataFrame, col: str) -> pd.DataFrame:
    def _del(row):
        for rx in row["rx_to_del"]:
            row["row"] = re.sub(rx, "", row["row"], flags=re.IGNORECASE)
        return row

    data["row"] = data[col].astype(str) + " "
    data = parse_rx(data)

    data = data.apply(_del, axis=1)
    return data
