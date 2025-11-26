"""Transformer template for Arkleg bill category selector."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.models.transformer_template import TransformerTemplate

BillCategoryTransformer: TransformerTemplate = {
    "base_url": empty_transform,
    "rel_url": empty_transform,
    "bill_cat_link": empty_transform,
}
