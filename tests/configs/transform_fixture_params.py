"""Params for auto-download of html fixtures
Each url will be downloaded if not already found locally
and saved into fixtures/{name}.{variant}.html

Inputs
- path: str (rel path from /fixtures/ and file name)
- variant: str (filename differentiator)

"""

LEG_TRANSFORM_PATHS = {
    (
        "leg/input_val",
        "happy",
    ),
}
