from server.db import example_movie_evaluator

DATASET_CONFIGS = {
    "movies": {
        "search_column": "primaryTitle",
        "primary_key": "tconst",
        "evaluation": example_movie_evaluator,
    }
}

DATASET_EVALUATORS = {
    "movies": example_movie_evaluator,
}

DATASET_SEARCH_COLUMNS = {
    "movies": "primaryTitle",
}

DATASET_PRIMARY_KEYS = {
    "movies": "tconst",
}
