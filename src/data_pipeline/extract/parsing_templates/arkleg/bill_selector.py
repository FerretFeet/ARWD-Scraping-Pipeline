"""Selector template for arkleg.state.ar.us/Bills/Detail?id=####."""

import html
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.data_pipeline.load.download_pdf import downloadPDF
from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.data_pipeline.transform.utils.strip_session_from_string import strip_session_from_link
from src.data_pipeline.transform.utils.transform_str_to_date import transform_str_to_date
from src.models.selector_template import SelectorTemplate
from src.structures import directed_graph
from src.structures.directed_graph import Node
from src.utils.strings.get_url_base_path import get_url_base_path


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
                "bill_no_dwnld": (_BillParsers.parse_bill_no_dwnld, empty_transform),
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
                "bill_documents": (
                    _BillParsers.parse_other_bill_documents,
                    _BillTransformers.transform_bill_documents,
                ),
                "bill_status_history": (
                    _BillParsers.parse_bill_status_history,
                    _BillTransformers.transform_bill_status_history,
                ),
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
                "state_download_pdfs": (self.download_doc_pdfs, empty_transform),
            },
        )

    def download_doc_pdfs(
        self,
        node: Node,
        state_tree: directed_graph.DirectionalGraph,
        parsed_data: dict,
    ) -> int | None:
        """
        Side effect - download the pdf files, replace urls with local paths.

        Created here because it seemed like the easiest place to insert it with the architecture.
        """
        bill_no = parsed_data.get("bill_no")  # hb1001
        base_url = get_url_base_path(node.url, include_path=False)
        if not bill_no:
            return None
        session_code = strip_session_from_link(node.url).replace("/", "_")
        category = re.match(r"^([a-zA-Z]+)", bill_no)
        if category:
            category = category.group(1)  # hb, sb, hjr, sjr

        targets: dict[str, list[str]] = parsed_data.get("bill_documents")
        if not targets:
            return None
        for key, val in targets.items():
            newbillno = bill_no + "_" + key
            newbillno = normalize_str(newbillno).replace(" ", "_")

            newpaths = []
            nval = val
            if not isinstance(val, list):
                nval = [val]
            for idx, v in enumerate(nval):
                lpath = (
                    downloadPDF(
                        session_code + "/" + category + "/" + bill_no,
                        newbillno + "_" + str(idx),
                        urljoin(base_url, v),
                    )
                    if v
                    else None
                )
                newpaths.append(str(lpath))
            parsed_data["bill_documents"][key] = newpaths
        return 0

    def state_sponsor_lookup(
        self,
        node: Node,
        state_tree: directed_graph.DirectionalGraph,
        parsed_data: dict,
        pdkey: str,
    ) -> dict[str, dict]:
        """
        Lookup sponsor id from state.

        Resolve legislators and committees.
        """
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
                state_tree,
                {rkey: None},
                {"url": html.unescape(url)},
            )
            if found_node:
                returndict.setdefault(rkey, []).append(found_node.data[rkey])
            else:
                # find closest match
                # sometimes links dont go to the right page
                returndict.setdefault(rkey, []).append(None)

        return {pdkey: returndict}

    def primary_sponsor_lookup(
        self,
        node: directed_graph.Node,
        state_tree: directed_graph.DirectionalGraph,
        parsed_data: dict,
    ) -> dict[str, dict]:
        """Lookup primary sponsor id."""
        return self.state_sponsor_lookup(node, state_tree, parsed_data, "lead_sponsor")

    def cosponsors_sponsor_lookup(
        self,
        node: directed_graph.Node,
        state_tree: directed_graph.DirectionalGraph,
        parsed_data: dict,
    ) -> dict[str, dict]:
        """Lookup cosponsors. id."""
        return self.state_sponsor_lookup(node, state_tree, parsed_data, "cosponsors")

    def other_primary_sponsor_lookup(
        self,
        node: directed_graph.Node,
        state_tree: directed_graph.DirectionalGraph,
        parsed_data: dict,
    ) -> dict[str, dict]:
        """Lookup other primary sponsor id."""
        return self.state_sponsor_lookup(node, state_tree, parsed_data, "other_primary_sponsor")


class _BillTransformers:
    """Methods for transforming data from ark.leg.bill_detail page."""

    @staticmethod
    def transform_bill_status_history(
        pinput: list[dict],
        *,
        strict: bool = False,
    ) -> list[dict]:
        """Transform bill status history, normalize strings and format date."""
        if not pinput:
            return None
        for item in pinput:
            for (
                k,
                v,
            ) in item.items():
                if k in ["chamber", "history_action"]:
                    item[k] = normalize_str(v)
                if k == "status_date":
                    item[k] = str(transform_str_to_date(v))
                if k == "vote_action_present":
                    item[k] = bool(v) if v else False
        return pinput

    @staticmethod
    def transform_bill_documents(
        dinput: dict,
        *,
        strict: bool = False,
    ) -> dict[str, dict]:
        """
        Transform bill documents.

        Strip and attach to dictionary.
        """
        returndict = {}
        for key, val in dinput.items():
            nkey = key.lower()
            nval = [v.strip() for v in val if v]
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
    def parse_bill_status_history(soup: BeautifulSoup) -> list[dict | None]:
        bill_status_history: list[dict | None] = []
        bill_status_header = soup.find(
            "h3",
            string=re.compile("Bill Status History", re.IGNORECASE),
        )
        if not bill_status_header:
            return bill_status_history
        bill_status_table = bill_status_header.find_next("div", id="tableDataWrapper")
        if not bill_status_table:
            return bill_status_history
        bill_statuses = bill_status_table.find_all("div", class_=["tableRow", "tableRowAlt"])
        for row in bill_statuses:
            status = {}
            cells = row.find_all("div")
            status.update({"chamber": cells[0].get_text(strip=True) if cells[0] else None})
            status.update({"status_date": cells[1].get_text(strip=True) if cells[1] else None})
            status.update({"history_action": cells[2].get_text(strip=True) if cells[2] else None})
            status.update(
                {"vote_action_present": cells[3].get_text(strip=True) if cells[3] else None},
            )
            bill_status_history.append(status)

        return bill_status_history

    @staticmethod
    def parse_other_bill_documents(soup: BeautifulSoup) -> dict[str, dict[str, list[str]]] | None:
        bill_documents = soup.find_all("a", attrs={"aria-label": "Download PDF"})
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
        return result[2] if result else None

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
