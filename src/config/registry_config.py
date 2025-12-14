
from src.config.pipeline_enums import PipelineRegistries, PipelineRegistryKeys
from src.data_pipeline.extract.fetching_templates import arkleg_fetchers
from src.data_pipeline.extract.fetching_templates.arkleg_fetchers import (
    ArkLegSeederLinkSelector,
    BillCategoryLinkSelector,
    BillLinkSelector,
    BillListLinkSelector,
    BillSectionLinkSelector,
    LegislatorListLinkSelector,
)
from src.data_pipeline.extract.parsing_templates.arkleg import parsing_templates
from src.data_pipeline.extract.parsing_templates.arkleg.bill_selector import BillSelector
from src.data_pipeline.extract.parsing_templates.arkleg.bill_vote_selector import BillVoteSelector
from src.data_pipeline.extract.parsing_templates.arkleg.legislator_list_selector import (
    LegislatorListSelector,
)
from src.data_pipeline.extract.parsing_templates.arkleg.legislator_selector import (
    LegislatorSelector,
)
from src.utils.paths import project_root

SQL_LOADER_BASE_PATH = project_root / "sql" / "dml" / "functions"

LOADER_CONFIG: dict = {
    PipelineRegistryKeys.BILL: {
        "params": {
            "title",
            "bill_no",
            "url",
            "session_code",
            "lead_sponsor",
        },
        "name": "Upsert Bill with Sponsors",
        "filepath": SQL_LOADER_BASE_PATH / "upsert_bill_with_sponsors.sql",
        "insert":"""
            SELECT upsert_bill_with_sponsors(
               p_title := %(p_title)s,
               p_bill_no := %(p_bill_no)s,
               p_url := %(p_url)s,
               p_session_code := %(p_session_code)s,
               p_intro_date := %(p_intro_date)s,
               p_act_date := %(p_act_date)s,
               p_bill_documents := %(p_bill_documents)s::jsonb,
               p_lead_sponsor := %(p_lead_sponsor)s::jsonb,
               p_other_primary_sponsor := %(p_other_primary_sponsor)s::jsonb,
               p_cosponsors := %(p_cosponsors)s::jsonb,
               p_bill_status_history := %(p_bill_status_history)s::jsonb[]
               ) AS bill_id;
        """,
    },

    PipelineRegistryKeys.BILL_VOTE: {
        "params": {
            "bill_id",
            "chamber",
        },
        "name": "Upsert Bill Vote",
        "filepath": SQL_LOADER_BASE_PATH / "upsert_bill_votes.sql",
        "insert": """
            SELECT upsert_bill_votes(
                p_bill_id := %(p_bill_id)s,
                p_vote_timestamp := %(p_vote_timestamp)s,
                p_chamber := %(p_chamber)s::chamber,
                p_motion_text := %(p_motion_text)s,
                p_yea_voters := %(p_yea_voters)s::JSONB,
                p_nay_voters := %(p_nay_voters)s::JSONB,
                p_non_voting_voters := %(p_non_voting_voters)s::JSONB,
                p_present_voters := %(p_present_voters)s::JSONB,
                p_excused_voters := %(p_excused_voters)s::JSONB
            );
        """,
    },

    PipelineRegistryKeys.LEGISLATOR: {
        "params": {
            "first_name",
            "last_name",
            "url",
            "district",
            "seniority",
            "chamber",
        },
        "name": "Upsert Legislator",
        "filepath": SQL_LOADER_BASE_PATH / "upsert_legislator.sql",
        "insert": """
            SELECT upsert_legislator(
                p_first_name     := %(p_first_name)s::text,
                p_last_name      := %(p_last_name)s::text,
                p_url            := %(p_url)s::text,
                p_phone          := %(p_phone)s::text,
                p_email          := %(p_email)s::text,
                p_address        := %(p_address)s::text,
                p_district       := %(p_district)s::text,
                p_seniority      := %(p_seniority)s::smallint,
                p_chamber        := %(p_chamber)s::chamber,
                p_party          := %(p_party)s::text,
                p_session_code   := %(p_session_code)s::text,
                p_committee_ids  := %(p_committee_ids)s::int[]
                ) AS legislator_id;
        """,
    },
    PipelineRegistryKeys.COMMITTEE: {
        "params": {
            "name",
        },
        "name": "Upsert Committee",
        "filepath": SQL_LOADER_BASE_PATH / "upsert_committee.sql",
        "insert": """
            SELECT upsert_committee(
                p_committee_id := %(p_committee_id)s::int,
                p_name         := %(p_name)s::text,
                p_url          := %(p_url)s::text,
                p_session_code := %(p_session_code)s::text
            ) AS committee_id;
""",
    },
    PipelineRegistryKeys.LEGISLATOR_LIST: {
        "params": {
            "active_urls",
        },
        "name": "Close nonpresent legislators",
        "filepath": SQL_LOADER_BASE_PATH / "upsert_committee.sql",
        "insert": """
            SELECT close_missing_legislators(
                p_active_urls  := %(p_active_urls)s::text[],
                p_session_code := %(p_session_code)s::text
            );
""",
    },
}


PROCESSOR_CONFIG: dict = {
    PipelineRegistryKeys.ARK_LEG_SEEDER: {
        PipelineRegistries.FETCH: ArkLegSeederLinkSelector,
    },
    PipelineRegistryKeys.BILLS_SECTION: {
        PipelineRegistries.FETCH: BillSectionLinkSelector,
    },
    PipelineRegistryKeys.BILL_CATEGORIES: {
        PipelineRegistries.FETCH: BillCategoryLinkSelector,
    },
    PipelineRegistryKeys.BILL_LIST: {
        PipelineRegistries.FETCH: BillListLinkSelector,
    },
    PipelineRegistryKeys.BILL: {
        PipelineRegistries.FETCH: BillLinkSelector,
        PipelineRegistries.PROCESS: BillSelector,
    },
    PipelineRegistryKeys.BILL_VOTE: {
        PipelineRegistries.PROCESS: BillVoteSelector,
    },
    PipelineRegistryKeys.LEGISLATOR_LIST: {
        PipelineRegistries.FETCH: LegislatorListLinkSelector,
        PipelineRegistries.PROCESS: LegislatorListSelector,
    },
    PipelineRegistryKeys.LEGISLATOR: {
        PipelineRegistries.PROCESS: LegislatorSelector,
    },
    PipelineRegistryKeys.COMMITTEES_CAT: {
        PipelineRegistries.FETCH: arkleg_fetchers.CommitteeCategories,
    },
    PipelineRegistryKeys.COMMITTEES_LIST: {
        PipelineRegistries.FETCH: arkleg_fetchers.CommitteeListLinkSelector,
    },
    PipelineRegistryKeys.COMMITTEE: {
        PipelineRegistries.PROCESS: parsing_templates.CommitteeSelector,
    },
}
