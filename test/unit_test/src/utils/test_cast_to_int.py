import pytest

from src.data_pipeline.transform.utils.cast_to_int import cast_to_int



class TestCastToInt:

    @pytest.mark.parametrize('input_str, output_num',
                            [
                                ('20', 20),
                                (20, 20),
                            ])
    def test_cast_to_int_success(self, input_str, output_num):
        result = cast_to_int(input_str)
        assert result == output_num

    def test_cast_to_int(self):
        with pytest.raises(ValueError):
            cast_to_int('Bad Input', strict=True)