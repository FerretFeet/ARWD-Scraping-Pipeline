"""Function for transforming legislator title."""

from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.utils.logger import logger

NUM_NAMES = 2


def transform_leg_title(title: list[str] | str, *, strict: bool = False) -> dict[str, str]:
    """
    Transform string like -> "Representative Aaron Pilkington (R)".

    Outputs:
    {f_name: str,
     l_name: str,
      chamber: str,
       party: str}
    """
    if isinstance(title, list):
        if len(title) > 1:
            msg = f"Expected one title in list, got {len(title)} :: {title} instead"
            logger.warning()
            if strict:
                raise ValueError(msg)
        title = title[0]

    result = {
        "party": _parse_party(title),
        "f_name": (_parse_name(title))[0],
        "l_name": (_parse_name(title))[1],
        "chamber": _parse_chamber(title),
    }
    return {
        "party": normalize_str(result["party"]) if result["party"] else None,
        "f_name": normalize_str(result["f_name"]) if result["f_name"] else None,
        "l_name": normalize_str(result["l_name"]) if result["l_name"] else None,
        "chamber": normalize_str(result["chamber"] if result["chamber"] else None),
    }


def _parse_party(title: str) -> str | None:
    if "(" and ")" in title:
        return title[title.find("(") + 1 : title.find(")")]
    return None


def _parse_chamber(title: str) -> str | None:
    words = title.split()
    house_chamber_strs = ["representative", "house"]
    senate_chamber_strs = ["senator", "senate"]
    if words and words[0].lower() in house_chamber_strs + senate_chamber_strs:
        chamber = words[0]
        if chamber.lower() in house_chamber_strs:
            return "house"
        if chamber.lower() in senate_chamber_strs:
            return "senate"
    return None


def _parse_name(title: str) -> tuple[str | None, str | None]:

    chamber = _parse_chamber(title)
    end_idx = title.find("(")
    name_part = title if end_idx == -1 else title[:end_idx]
    return_val = (None, None)
    if chamber:
        words = name_part.split()
        name_words = words[1:]  # remove chamber
        name = " ".join(name_words)
    else:
        name_words = name_part.split()
        name = " ".join(name_words)
    if not name:
        return return_val
    split_names = name.split(" ", maxsplit=1)
    if not split_names:
        return return_val
    if len(split_names) == NUM_NAMES:
        f_name = split_names[0]
        l_name = split_names[1]
    else:
        f_name = None
        l_name = split_names[0]
    return f_name, l_name
