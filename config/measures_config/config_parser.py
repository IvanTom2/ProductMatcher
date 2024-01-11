from enum import Enum


class CONFIG(object):
    NAME = "config_name"
    MEASURES = "measures"

    NUMERIC_MEASURES = "numeric_measures"
    STRING_MEASURES = "string_measures"
    COMPLEX_MEASURES = "complex_measures"

    USE_IT = "use_it"

    MEASURE_TYPES = [
        NUMERIC_MEASURES,
        STRING_MEASURES,
    ]


class AUTOSEM_CONF(object):
    MERGE_MODE = "merge_mode"
    EXCLUDE_RX = "exclude_rx"
    USE_IT = "use_it"


class TEXT_FEATURES_CONF:
    USE_IT = "use_it"
    VALIDATION_MODE = "validation_mode"
    NOT_FOUND_MODE = "not_found_mode"
    PRIORITY = "priority"


class MEASURE(object):
    NAME = "measure_name"
    DATA = "measure_data"

    AUTOSEM = "autosem"
    TEXT_FEATURES = "feature_validator"

    AUTOSEM_CONF = AUTOSEM_CONF
    TEXT_FEATURES_CONF = TEXT_FEATURES_CONF


class DATA(object):
    COMMON_PREFIX = "common_prefix"
    COMMON_POSTFIX = "common_postfix"
    COMMON_MAX_COUNT = "common_max_count"
    SPECIAL_VALUE_SEARCH = "special_value_search"
    FEATURES = "features"


class FEATURE(object):
    NAME = "feature_name"
    DEFENITION = "defenition"
    RWEIGHT = "relative_weight"
    PREFIX = "prefix"
    POSTFIX = "postfix"
    MAX_COUNT = "max_count"
    SEARCH_MODE = "search_mode"
    USE_IT = "use_it"


class MEASURE_MAPPER(object):
    weight = "Вес"
    volume = "Объем"
    quantity = "Количество"

    color = "Цвет"
