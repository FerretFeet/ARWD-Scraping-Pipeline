
from src.config.pipeline_enums import PipelineRegistries, PipelineRegistryKeys
from src.data_pipeline.extract.fetching_templates.arkleg_fetchers import (
    ArkLegSeederLinkSelector,
    BillCategoryLinkSelector,
    BillLinkSelector,
    BillListLinkSelector,
    BillSectionLinkSelector,
    LegislatorListLinkSelector,
)
from src.data_pipeline.extract.parsing_templates.arkleg.bill_category_selector import (
    BillCategorySelector,
)
from src.data_pipeline.extract.parsing_templates.arkleg.bill_list_selector import BillListSelector
from src.data_pipeline.extract.parsing_templates.arkleg.bill_selector import BillSelector
from src.data_pipeline.extract.parsing_templates.arkleg.bill_vote_selector import BillVoteSelector
from src.data_pipeline.extract.parsing_templates.arkleg.legislator_list_selector import (
    LegislatorListSelector,
)
from src.data_pipeline.extract.parsing_templates.arkleg.legislator_selector import (
    LegislatorSelector,
)
from src.utils.paths import project_root

SQL_LOADER_BASE_PATH = project_root / "sql" / "dml"

LOADER_CONFIG: dict = {

    # PipelineRegistryKeys.BILL: {
    #     "params": {
    #         #Required parameters
    #     },
    #     "filepath": SQL_LOADER_BASE_PATH / "",
    # },
    #
    # PipelineRegistryKeys.BILL_VOTE: {
    #     "params": {},
    #     "filepath": SQL_LOADER_BASE_PATH / "",
    # },
    #
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
        PipelineRegistries.PROCESS: BillCategorySelector,
    },

    PipelineRegistryKeys.BILL_LIST: {
        PipelineRegistries.FETCH: BillListLinkSelector,
        PipelineRegistries.PROCESS: BillListSelector,
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
}
