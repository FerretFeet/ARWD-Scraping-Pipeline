"""Transformer template for Arkleg bill category selector."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.first_el_from_list import first_el_from_list
from src.models.transformer_template import TransformerTemplate

BillCategoryTransformer: TransformerTemplate = {
    "base_url": first_el_from_list,
    "rel_url": first_el_from_list,
    "bill_cat_link": empty_transform,
}
