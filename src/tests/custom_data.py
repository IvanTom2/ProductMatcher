import pandas as pd

from common_test import CLIENT_PRODUCT, SOURCE_PRODUCT, IS_EQUAL


class CustomData(object):
    @classmethod
    def custom_weight_data(cls) -> pd.DataFrame:
        data = pd.DataFrame(
            data=[
                ("Вес 10мкг", "Set of 10 ugly bannies", False),
                ("Вес 10грамм", "Бочка 10 галлонов ", False),
                ("Вес 10грамм", "Порошок 10 гранул", False),
                ("Вес 10грамм", "Set 10game", False),
                ("Вес 10грамм", "Book 10grammar", False),
                ("Вес 10грамм", "Аспирин 10гр/1мл", False),
                ("Вес 10грамм", "Аспирин 10g/2ml", False),
                ("Вес 10мкг", "Аспирин 10мкг\\1мл", False),
                ("Вес 10мг", "Аспирин 10мг\\1мл", False),
                ("Вес 10гр", "Аспирин 10гр\\1мл", False),
                ("Вес 10кг", "Аспирин 10кг\\1мл", False),
                ("Вес 10мкг", "Аспирин 10мкг/1мл", False),
                ("Вес 10мг", "Аспирин 10мг/1мл", False),
                ("Вес 10гр", "Аспирин 10гр/1мл", False),
                ("Вес 10кг", "Аспирин 10кг/1мл", False),
                ("Вес 10грамм", "Вес 10г.", True),
                ("Вес 10грамм", "Вес 10gr", True),
            ],
            columns=[CLIENT_PRODUCT, SOURCE_PRODUCT, IS_EQUAL],
        )
        return data

    @classmethod
    def custom_volume_data(cls) -> pd.DataFrame:
        data = pd.DataFrame(
            data=[
                ("Объем 10 литров", "Набор 10 лимонов", False),
                ("Объем 10 литров", "Movie 10 losers", False),
                ("Объем 10 мл", "Аспирин 1г/10мл", False),
                ("Объем 10 мл", "Аспирин 1г\\10мл", False),
                ("Объем 10 мл", "Аспирин 1г  /10мл", False),
                ("Объем 10 мл", "Аспирин 1г  \\10мл", False),
                ("Объем 10 литров", "Объем 10 litres", True),
                ("Объем 10 литров", "Объем 10 liters", True),
                ## Not working test complex testcases
                ## Can be fixed with adding "(?<![\\\/]\s*)" expr
                ## Need to mix it with current prefix constraints
                # ("Объем 10 мл", "Аспирин 1г/  10мл", False),
                # ("Объем 10 мл", "Аспирин 1г\\  10мл", False),
                # ("Объем 10 мл", "Аспирин 1г  /  10мл", False),
                # ("Объем 10 мл", "Аспирин 1г  \\  10мл", False),
            ],
            columns=[CLIENT_PRODUCT, SOURCE_PRODUCT, IS_EQUAL],
        )
        return data

    @classmethod
    def custom_quantity_data(cls) -> pd.DataFrame:
        data = pd.DataFrame(
            data=[
                ("Количество 10шт", "Column 10", False),
                ("Количество 10шт", "SpaceX 10 rockets", False),
                ("Количество 10шт", "Мех 10 пробы", False),
                ("Количество 10шт", "Сани 10 упряжек", False),
                ("Количество 10шт", "Коробка масла 10 пачули", False),
                ("Количество 10шт", "Количество N10", True),
                ("Количество №10", "Количество 10шт", True),
            ],
            columns=[CLIENT_PRODUCT, SOURCE_PRODUCT, IS_EQUAL],
        )

        return data

    @classmethod
    def custom_memory_capacity_data(cls) -> pd.DataFrame:
        data = pd.DataFrame(
            data=[
                ("Память 1тб", "Память 1000гигабайтов", True),
                ("Память 1тб", "Память 1000000мегабайтов", True),
            ],
            columns=[CLIENT_PRODUCT, SOURCE_PRODUCT, IS_EQUAL],
        )

        return data

    @classmethod
    def custom_concentration_per_dose_data(cls) -> pd.DataFrame:
        data = pd.DataFrame(
            data=[
                ("Лекарство 1001мг/доза", "Лекарство 1г/доза", False),
                ("Лекарство 1001мг\\доза", "Лекарство 1г/доза", False),
            ],
            columns=[CLIENT_PRODUCT, SOURCE_PRODUCT, IS_EQUAL],
        )

        return data

    @classmethod
    def custom_length_data(cls) -> pd.DataFrame:
        data = pd.DataFrame(
            data=[
                ("Длина 10м", "Длина 10мм", False),
                ("Длиан 10м", "Длина 10cm", False),
            ],
            columns=[CLIENT_PRODUCT, SOURCE_PRODUCT, IS_EQUAL],
        )

        return data


class CustomUncreationData(object):
    @classmethod
    def uncreation_weight_data(cls) -> pd.DataFrame:
        data = pd.DataFrame(
            data=[
                "Комплект 10 ugley",
                "Айфон 10 гигабайт",
                "Айфон 10 gigabyte",
            ],
            columns=[CLIENT_PRODUCT],
        )
        return data


if __name__ == "__main__":
    print(CustomData.custom_weight_data())
