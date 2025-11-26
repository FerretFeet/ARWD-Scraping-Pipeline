"""Params for auto-download of html fixtures
Each url will be downloaded if not already found locally
and saved into fixtures/{name}.{variant}.html

Inputs
- path: str (rel path from /fixtures/ and file name)
- variant: str (filename differentiator)

"""

leg_input_val = "leg/input_val"
LEG_VALIDATE_PATHS = {
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

leg_list_input_val = "leg_list/input_val"
LEG_LIST_VALIDATE_PATHS = {
    (
        leg_list_input_val,
        "happy",
        None,
    ),
}

bill_cat_input_val = "bill_cat/input_val"
BILL_CAT_VALIDATE_PATHS = {
    (
        bill_cat_input_val,
        "happy",
        None,
    ),
    (
        bill_cat_input_val,
        "bad_bill_cats",
        ValueError,
    ),
}

bill_list_input_val = "bill_list/input_val"
BILL_LIST_VALIDATE_PATHS = {
    (
        bill_list_input_val,
        "happy",
        None,
    ),
}

bill_input_val = "bill/input_val"
BILL_VALIDATE_PATHS = {
    (
        bill_input_val,
        "happy",
        None,
    ),
}

bill_vote_input_val = "bill_vote/input_val"
BILL_VOTE_VALIDATE_PATHS = {
    (
        bill_vote_input_val,
        "happy",
        None,
    ),
}
