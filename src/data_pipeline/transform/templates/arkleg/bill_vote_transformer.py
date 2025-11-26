"""Transformer for arkleg bill vote selector."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_list_of_str import normalize_list_of_str
from src.data_pipeline.transform.utils.normalize_str import normalize_str

BillVoteTransformer = {
    "base_url": empty_transform,
    "rel_url": empty_transform,
    "title": normalize_str,
    "yea_names": normalize_list_of_str,
    "yea_links": empty_transform,
    "nay_names": normalize_list_of_str,
    "nay_links": empty_transform,
    "non_voting_names": normalize_list_of_str,
    "non_voting_links": empty_transform,
    "present_names": normalize_list_of_str,
    "present_links": empty_transform,
    "excused_names": normalize_list_of_str,
    "excused_links": empty_transform,
}
