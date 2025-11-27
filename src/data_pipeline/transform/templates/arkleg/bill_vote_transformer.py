"""Transformer for arkleg bill vote selector."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_list_of_str_link import normalize_list_of_str_link
from src.data_pipeline.transform.utils.normalize_str import normalize_str

BillVoteTransformer = {
    "base_url": empty_transform,
    "rel_url": empty_transform,
    "title": normalize_str,
    "yea_voters": normalize_list_of_str_link,
    "nay_voters": normalize_list_of_str_link,
    "non_voting_voters": normalize_list_of_str_link,
    "present_voters": normalize_list_of_str_link,
    "excused_voters": normalize_list_of_str_link,
}
