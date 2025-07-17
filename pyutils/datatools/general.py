import abc
from typing import Any, Callable, Iterable, Optional


def get_in(
    coll: Iterable,
    keys: Iterable[str],
    default: Optional[Any] = None,
    formatter: Optional[Callable] = None,
):
    """
    Retrieves a value from a nested structure, where keys is the in-order
    sequence of keys or indexes needed to get the value from the nested structure.
    Inspired by: https://clojuredocs.org/clojure.core/get-in
    """
    coll_or_value = coll
    for key in keys:
        if isinstance(coll_or_value, list) and isinstance(key, int):
            try:
                coll_or_value = coll_or_value[key]
            except IndexError:
                coll_or_value = default
                break
        elif isinstance(coll_or_value, Iterable) and key in coll_or_value:
            coll_or_value = coll_or_value[key]
        else:
            return default

    # If you want the value being retrieved from the structure to be additionally
    # formatted in any way, the formatter argument is only called when a value is
    # successfully retrieved (i.e. not called on the default)
    if formatter and coll_or_value:
        coll_or_value = formatter(coll_or_value)

    return coll_or_value
