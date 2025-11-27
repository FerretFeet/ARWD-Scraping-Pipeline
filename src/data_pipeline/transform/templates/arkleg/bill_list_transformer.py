"""Transformer template for Arkleg Bill List Selector."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.models.transformer_template import TransformerTemplate

BillListTransformer: TransformerTemplate = {
    "base_url": empty_transform,
    "rel_url": empty_transform,
    "chamber": lambda chamber, *, strict: (
        normalize_str(chamber, strict=strict, remove_substr="bills")
    ),
    "session": empty_transform,
    "bill_url": empty_transform,
    "next_page": empty_transform,
}
