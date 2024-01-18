import sys
from pathlib import Path
import pandas as pd
from fuzzywuzzy import process as fuzz_process
from tqdm import tqdm

tqdm.pandas()

PROJECT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_DIR))

from src.fuzzy_v.tokenization import Token, TokenTransformer


def setup_tasks(
    search_func: callable,
    index: int,
    data: pd.DataFrame,
    left_tokens_column: str,
    right_tokens_column: str,
    transformer: TokenTransformer,
    fuzzy_threshold: int,
) -> pd.DataFrame:
    if not index:  # that mean index == 0
        data = data.progress_apply(
            search_func,
            args=(
                left_tokens_column,
                right_tokens_column,
                transformer,
                fuzzy_threshold,
            ),
            axis=1,
        )

    else:
        data = data.apply(
            search_func,
            args=(
                left_tokens_column,
                right_tokens_column,
                transformer,
                fuzzy_threshold,
            ),
            axis=1,
        )

    return data


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

    def _search_func(
        self,
        row: list[Token],
    ) -> tuple[list[Token]]:
        left_tokens: list[Token] = row[0]
        right_tokens: list[Token] = row[1]
        right_tokens_values = [token.value for token in right_tokens]

        for left_token in left_tokens:
            if left_token in right_tokens:
                index = right_tokens.index(left_token.value)
                right_token = right_tokens[index]

                self.transformer.transform(right_token, left_token, False)

            else:
                token_value, score = fuzz_process.extractOne(
                    left_token.value,
                    right_tokens_values,
                )
                if score >= self.fuzzy_threshold:
                    index = right_tokens.index(token_value)
                    right_token = right_tokens[index]

                    self.transformer.transform(right_token, left_token, True)

        return left_tokens, right_tokens

    def search(
        self,
        data: pd.DataFrame,
        left_tokens_column: str,
        right_tokens_column: str,
    ) -> pd.DataFrame:
        left_tokens = data[left_tokens_column].to_list()
        right_tokens = data[right_tokens_column].to_list()

        massive = list(zip(left_tokens, right_tokens))
        massive = list(map(self._search_func, tqdm(massive)))

        data[left_tokens_column] = list(map(lambda x: x[0], massive))
        data[right_tokens_column] = list(map(lambda x: x[1], massive))
        return data
