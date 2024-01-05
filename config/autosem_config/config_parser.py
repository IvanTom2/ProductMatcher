class CONFIG(object):
    NAME = "config_name"
    MEASURES = "measures"


class MEASURE(object):
    NAME = "measure_name"
    MERGE_MODE = "merge_mode"
    DATA = "measure_data"
    USE_IT = "use_it"


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
