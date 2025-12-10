"""Selector template for arkleg.state.ar.us/Bills/Detail?id=####."""

import html
import re

from bs4 import BeautifulSoup

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.data_pipeline.transform.utils.transform_str_to_date import transform_str_to_date
from src.models.selector_template import SelectorTemplate
from src.structures.directed_graph import Node


class BillSelector(SelectorTemplate):
    """Selector for Arkleg bill page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "title": ("div h1", normalize_str),
                "bill_no": (
                    _BillParsers.parse_bill_no,
                    lambda bill_no, *, strict: (
                        normalize_str(bill_no, strict=strict, remove_substr="PDF")
                    ),
                ),
                "act_no": (
                    _BillParsers.parse_act_no,
                    lambda bill_no, *, strict: (
                        normalize_str(bill_no, strict=strict, remove_substr="PDF")
                    ),
                ),
                "act_no_dwnld": (_BillParsers.parse_act_no_dwnld, empty_transform),
                "orig_chamber": (_BillParsers.parse_orig_chamber, normalize_str),
                "lead_sponsor": (_BillParsers.parse_lead_sponsor, empty_transform),
                "other_primary_sponsor": (
                    _BillParsers.parse_other_primary_sponsor,
                    empty_transform,
                ),
                "cosponsors": (_BillParsers.parse_cosponsors, empty_transform),
                "intro_date": (_BillParsers.parse_intro_date, transform_str_to_date),
                "act_date": (_BillParsers.parse_act_date, transform_str_to_date),
                "bill_documents": (_BillParsers.parse_other_bill_documents,
                                   _BillTransformers.transform_bill_documents),
                "state_primary_sponsor": (
                    self.primary_sponsor_lookup,
                    empty_transform,
                ),
                "state_cosponsors": (
                    self.cosponsors_sponsor_lookup,
                    empty_transform,
                ),
                "state_other_primary_sponsor": (
                    self.other_primary_sponsor_lookup,
                    empty_transform,
                ),
            },
        )

    def state_sponsor_lookup(self, node: Node, state_tree, parsed_data, pdkey):
        urls = parsed_data.get(pdkey)
        if not urls:
            return {pdkey: {}}

        returndict = {}
        for url in urls:
            if "committee" in url.lower():
                rkey = "committee_id"
            elif "legislator" in url.lower():
                rkey = "legislator_id"
            else:
                return None

            found_node = self.get_dynamic_state(
                node,
                state_tree,
                {rkey: None},
                {"url": html.unescape(url)},
            )
            if found_node:
                returndict.setdefault(rkey, []).append(found_node.data[rkey])
            else:
                return None

        return {pdkey: returndict}

    def primary_sponsor_lookup(self, node, state_tree, parsed_data):
        return self.state_sponsor_lookup(node, state_tree, parsed_data, "lead_sponsor")

    def cosponsors_sponsor_lookup(self, node, state_tree, parsed_data):
        return self.state_sponsor_lookup(node, state_tree, parsed_data, "cosponsors")

    def other_primary_sponsor_lookup(self, node, state_tree, parsed_data):
        return self.state_sponsor_lookup(node, state_tree, parsed_data, "other_primary_sponsor")


class _BillTransformers:
    @staticmethod
    def transform_bill_documents(dinput: dict, *, strict: bool = False):
        returndict = {}
        for key, val in dinput.items():
            nkey = key.lower()
            nval = [normalize_str(v) for v in val]
            returndict.update({nkey: nval})
        return {"bill_documents": returndict}
    @staticmethod
    def normalize_bill_doc_type(billdoc: str) -> str:
        if billdoc == "amendments":
            return "amendment"
        if "fiscal impact" in billdoc:
            return "fiscal_impact"
        return billdoc


class _BillParsers:
    @staticmethod
    def parse_other_bill_documents(soup: BeautifulSoup) -> dict[str, dict[str, list[str]]] | None:
        bill_documents = soup.find_all("a", attrs={"aria-label": "Download PDF"})
        if not bill_documents: return None
        result = {}
        for doc in bill_documents:
            label = doc.find_previous("h3")
            label = label.get_text()

            if label not in result:
                result[label] = [html.unescape(doc.get("href"))]
            else:
                result[label].append(html.unescape(doc.get("href")))
        result.update({"bill_text": _BillParsers.parse_bill_no_dwnld(soup)})
        result.update({"act_text": _BillParsers.parse_act_no_dwnld(soup)})
        return result

    @staticmethod
    def parse_vote_links(soup: BeautifulSoup) -> list[str] | None:
        vote_links = soup.find_all("a", string=re.compile(r"vote", re.IGNORECASE))  # type: ignore  # noqa: PGH003

        return [html.unescape(link.get("href")) for link in vote_links]

    @staticmethod
    def _parse_bill_detail_table(  # noqa: D417
        soup: BeautifulSoup,
        label_text: str,
        target_attr: str,
        *,
        nested_tag: str | None = None,
        additional_attrs: list[str] | None = [None],  # noqa: B006
    ) -> list[str] | str | None | tuple:
        """
        Find the corrosponding element.attr for the tabel label.

        Args:
         - soup: Beautiful Soup object
         - label_text: text label for table
         - target_attr: attribute to return. 'text' or None for text
         - nested_tag: If target el is buried in label_text element's sibling, drill this
         - additional_attrs: List of additional attrs to return in a Tuple

        """
        label_el = soup.find(name="div", string=re.compile(re.escape(label_text), re.IGNORECASE))  # type: ignore  # noqa: PGH003
        if label_el is None:
            return None
        target_el = label_el.find_next_sibling("div")
        if target_el is None:
            return None
        if nested_tag is not None:
            target_el = target_el.find_all(nested_tag)
        # Element successfully selected, extract data
        result = []
        for el in target_el:
            el_attrs = {}
            for attr in [*additional_attrs, target_attr]:
                if attr is None:
                    continue
                if attr == "text":
                    text = el.get_text(strip=True)
                    el_attrs[attr] = text
                else:
                    el_attrs[attr] = el.get(attr)
            result.append(el_attrs)

        attr_order = ["text"] + [
            a for a in [*additional_attrs, target_attr] if a != "text" and a is not None
        ]

        if all(len(result) == 1 for result in result):
            # Flatten dict if only 1 key
            result = [next(iter(x.values())) for x in result]
        else:
            result = [tuple(x[attr] for attr in attr_order) for x in result]

        if len(result) == 1:
            # return list entry if only item in result.
            result = result[0]

        return result

    @staticmethod
    def parse_bill_no(soup: BeautifulSoup) -> str | None:
        label = "Bill Number"
        result = _BillParsers._parse_bill_detail_table(soup, label, "text")
        return result[2]

    @staticmethod
    def parse_bill_no_dwnld(soup: BeautifulSoup) -> list[str] | None:
        label = "Bill Number"
        return [_BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")]

    @staticmethod
    def parse_act_no(soup: BeautifulSoup) -> str | None:
        label = "Act Number"
        result = _BillParsers._parse_bill_detail_table(soup, label, "text")
        return result[2]

    @staticmethod
    def parse_act_no_dwnld(soup: BeautifulSoup) -> list[str] | None:
        label = "Act Number"
        return [_BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")]

    @staticmethod
    def parse_orig_chamber(soup: BeautifulSoup) -> str | None:
        label = "Originating Chamber"
        return _BillParsers._parse_bill_detail_table(soup, label, "text")

    @staticmethod
    def parse_lead_sponsor(soup: BeautifulSoup) -> list[tuple[str, str]] | None:
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(
            soup,
            label,
            "href",
            nested_tag="a",
        )

    @staticmethod
    def __parse_lead_sponsor_link(soup: BeautifulSoup) -> list[str] | str | None:
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_other_primary_sponsor(soup: BeautifulSoup) -> list[tuple[str, str]] | None:
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(
            soup,
            label,
            "href",
            nested_tag="a",
        )

    @staticmethod
    def __parse_other_primary_sponsor_link(soup: BeautifulSoup) -> list[str] | None:
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_cosponsors(soup: BeautifulSoup) -> list[tuple[str, str]] | None:
        label = "Cosponsors"
        return _BillParsers._parse_bill_detail_table(
            soup,
            label,
            "href",
            nested_tag="a",
        )

    @staticmethod
    def __parse_cosponsors_link(soup: BeautifulSoup) -> list[str] | None:
        label = "Cosponsors"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_intro_date(soup: BeautifulSoup) -> str | None:
        label = "Introduction Date"
        return _BillParsers._parse_bill_detail_table(soup, label, "text")

    @staticmethod
    def parse_act_date(soup: BeautifulSoup) -> str | None:
        label = "Act Date"
        return _BillParsers._parse_bill_detail_table(soup, label, "text")
