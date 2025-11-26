"""Transformer template for Arkleg bill selector."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_list_of_str import normalize_list_of_str
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.data_pipeline.transform.utils.transform_str_to_date import transform_str_to_date
from src.models.transformer_template import TransformerTemplate

BillTransformer: TransformerTemplate = {
    "base_url": empty_transform,
    "rel_url": empty_transform,
    "title": normalize_str,
    "bill_no": lambda bill_no: (normalize_str(bill_no).replace("PDF", "i")),
    "bill_no_dwnld": empty_transform,
    "act_no": lambda bill_no: (normalize_str(bill_no).replace("PDF", "i")),
    "act_no_dwnld": empty_transform,
    "orig_chamber": normalize_str,
    "lead_sponsor": normalize_list_of_str,
    "lead_sponsor_link": empty_transform,
    "other_primary_sponsor": normalize_list_of_str,
    "other_primary_sponsor_link": empty_transform,
    "cosponsors": normalize_list_of_str,
    "cosponsors_link": empty_transform,
    "intro_date": transform_str_to_date,
    "act_date": transform_str_to_date,
    "vote_links": empty_transform,
}
