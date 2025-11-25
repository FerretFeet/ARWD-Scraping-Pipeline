import pytest

from src.data_pipeline.transform.PipelineTransformer import PipelineTransformer

fake_content = {"text": "SHOULD CONVERT TO LOWERCASE"}

fake_template = {"text": lambda val: {"text": val.lower()}}


class TestTransformContent:
    def test_transform_content_success(self):
        transformer = PipelineTransformer(True)
        result = transformer.transform_content(fake_template, fake_content)
        assert result is not None
        assert result["text"] == fake_content["text"].lower()

    @pytest.mark.parametrize(
        "bad_transformer",
        [
            {"text": "invalid_selector_type"},
            {"text": lambda val: {"incorrect return type"}},
            {"bad_key": lambda val: {val.lower()}},
        ],
    )
    def test_transform_content_raises_exception_when_strict(self, bad_transformer):
        """
        Ensure invalid types raise TypeError
        template.items() should be callables that return dicts
        """
        bad_template = {"text": bad_transformer}
        transformer = PipelineTransformer(True)
        with pytest.raises(Exception):
            result = transformer.transform_content(bad_template, fake_content)
            assert not result
