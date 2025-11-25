from typing import Dict

from src.data_pipeline.transform.utils.cast_to_int import cast_to_int
from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_list_of_str import normalize_list_of_str
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.data_pipeline.transform.utils.transform_leg_title import transform_leg_title
from src.data_pipeline.transform.utils.transform_phone import transform_phone
from src.models.TransformerTemplate import TransformerTemplate


LegislatorTransformer: TransformerTemplate = {
    'base_url': empty_transform,
    'rel_url': empty_transform,
    'title': transform_leg_title,
    'phone': transform_phone,
    'email': normalize_str,
    'address': normalize_str,
    'district': cast_to_int,
    'seniority': cast_to_int,
    'public_service': normalize_str,
    'committees': normalize_list_of_str,
    'committee_links': empty_transform
}


