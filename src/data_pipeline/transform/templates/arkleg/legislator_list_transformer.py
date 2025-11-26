"""Transformer template for Arkleg Legislator List selector."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.models.transformer_template import TransformerTemplate

LegislatorListTransformer: TransformerTemplate = {
    "base_url": empty_transform,
    "rel_url": empty_transform,
    "legislator_link": empty_transform,
}
