import collections
import copy

from tabulate import tabulate
from termcolor import colored


def dict_deep_merge(dct1, dct2, override=True) -> dict:
    merged = copy.deepcopy(dct1)
    for k, v2 in dct2.items():
        if k in merged:
            v1 = merged[k]
            if isinstance(v1, dict) and isinstance(v2, collections.Mapping):
                merged[k] = dict_deep_merge(v1, v2, override)
            elif isinstance(v1, list) and isinstance(v2, list):
                merged[k] = v1 + v2
            else:
                if override:
                    merged[k] = copy.deepcopy(v2)
        else:
            merged[k] = copy.deepcopy(v2)
    return merged


def title(text: str):
    return text + "\n" + ("=" * round(len(text)))

def sign_color(string: str, number: float) -> str:
    if number == 0:
        return string
    else:
        return colored(string, "red" if number > 0 else "green")

def table(table, **kwargs):
    return tabulate(table, tablefmt="fancy_grid", floatfmt=".2f", **kwargs)