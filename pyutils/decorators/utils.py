import random
from typing import Union


def get_jitter_delay_value(
    attempt: int,
    base: int = 4,
    min_delay: int = 2,
    max_delay: int = 12,
    return_int: bool = False,
    return_float_precision: int = 4,
) -> Union[int, float]:
    """
    Determines the new jitter value to use for a delay.  This will calculate a
    new_delay based on the attempt - the higher the attempt, the higher the new_delay.
    This will then return a random value between the min_delay and new_delay.  If the
    new_delay is bigger than the max_delay, the max_delay will be used.  This adds a
    randomizing element to exponential backoff, so fewer clients would retry at the
    same time:
    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """

    # Ensure required variables are not none
    base = 4 if base is None else base
    min_delay = 2 if min_delay is None else min_delay
    max_delay = 12 if max_delay is None else max_delay

    new_delay = min(max_delay, base * 2**attempt)
    if return_int:
        return random.randint(min_delay, new_delay)

    return round(random.uniform(min_delay, new_delay), return_float_precision)
