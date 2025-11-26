"""Params for auto-download of html fixtures
Each url will be downloaded if not already found locally
and saved into fixtures/{name}.{variant}.html

Inputs
- path: str (rel path from /fixtures/ and file name)
- variant: str (filename differentiator)

"""

leg_input_val = "leg/input_val"
LEG_TRANSFORM_PATHS = {
    (
        leg_input_val,
        "happy",
        None,
    ),
    (
        leg_input_val,
        "bad_values",
        None,
    ),
    (
        leg_input_val,
        "extra_keys",
        KeyError,
    ),
    (
        leg_input_val,
        "missing_keys",
        None,
    ),
}
