from src.data_pipeline.transform.utils.normalize_list_of_str_link import normalize_list_of_str_link


def test_empty_input_returns_none():
    assert normalize_list_of_str_link([]) is None


def test_normalize_single_pair(monkeypatch):
    # Patch normalize_str and empty_transform to identity functions
    monkeypatch.setattr(
        "src.data_pipeline.transform.utils.normalize_str.normalize_str",
        lambda s, strict=False: s.lower(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.data_pipeline.transform.utils.empty_transform.empty_transform",
        lambda s, strict=False: s or "",  # noqa: ARG005
    )

    input_list = [("ABC", "Link1")]
    expected = [("abc", "Link1")]
    assert normalize_list_of_str_link(input_list) == expected


def test_filter_out_empty_items(monkeypatch):
    monkeypatch.setattr(
        "src.data_pipeline.transform.utils.normalize_str.normalize_str",
        lambda s, strict=False: s,  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.data_pipeline.transform.utils.empty_transform.empty_transform",
        lambda s, strict=False: s or "",  # noqa: ARG005
    )

    input_list = [("valid", "link"), ("", "link"), ("valid2", None)]
    expected = [("valid", "link")]
    assert normalize_list_of_str_link(input_list) == expected


def test_multiple_pairs(monkeypatch):
    monkeypatch.setattr(
        "src.data_pipeline.transform.utils.normalize_str.normalize_str",
        lambda s, strict=False: s.upper(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.data_pipeline.transform.utils.empty_transform.empty_transform",
        lambda s, strict=False: s[::-1],  # noqa: ARG005
    )

    input_list = [("abc", "123"), ("def", "456")]
    expected = [("abc", "123"), ("def", "456")]
    assert normalize_list_of_str_link(input_list) == expected
