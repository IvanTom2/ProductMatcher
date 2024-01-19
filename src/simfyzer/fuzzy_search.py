import sys
import multiprocessing
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from functools import partial
from fuzzywuzzy import process as fuzz_process

tqdm.pandas()

PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from src.simfyzer.tokenization import Token, TokenTransformer


def searching_func(
    row: list[Token],
    transformer: TokenTransformer,
    fuzzy_threshold: int,
) -> tuple[list[Token]]:
    left_tokens: list[Token] = row[0]
    right_tokens: list[Token] = row[1]
    right_tokens_values = [token.value for token in right_tokens]

    for left_token in left_tokens:
        if left_token in right_tokens:
            index = right_tokens.index(left_token.value)
            right_token = right_tokens[index]

            transformer.transform(right_token, left_token, False)

        else:
            token_value, score = fuzz_process.extractOne(
                left_token.value,
                right_tokens_values,
            )
            if score >= fuzzy_threshold:
                index = right_tokens.index(token_value)
                right_token = right_tokens[index]

                transformer.transform(right_token, left_token, True)

    return left_tokens, right_tokens


class FuzzySearch(object):
    def __init__(
        self,
        fuzzy_threshold: int,
        transformer: TokenTransformer,
    ) -> None:
        if fuzzy_threshold > 1 or fuzzy_threshold < 0:
            raise ValueError("Fuzzy threshold should be in range 0 to 1")
        self.fuzzy_threshold = fuzzy_threshold * 100
        self.transformer = transformer

        self._process_pool = None

    def search(
        self,
        data: pd.DataFrame,
        left_tokens_column: str,
        right_tokens_column: str,
        process_pool: multiprocessing.Pool = None,
    ) -> pd.DataFrame:
        self._process_pool = process_pool

        left_tokens = data[left_tokens_column].to_list()
        right_tokens = data[right_tokens_column].to_list()

        massive = list(zip(left_tokens, right_tokens))
        search_func = partial(
            searching_func,
            transformer=self.transformer,
            fuzzy_threshold=self.fuzzy_threshold,
        )

        if self._process_pool != None:
            massive = self._process_pool.map(search_func, tqdm(massive))
        else:
            massive = list(map(search_func, tqdm(massive)))

        data[left_tokens_column] = list(map(lambda x: x[0], massive))
        data[right_tokens_column] = list(map(lambda x: x[1], massive))

        return data
