"""
Convert dbt docs(manifest.json and catalog.json) to markdown file.

Inspired by https://github.com/rfdearborn/dbt-docs-to-notion/blob/main/dbt_docs_to_notion.py

manifest.json specification: https://docs.getdbt.com/reference/artifacts/manifest-json
catalog.json specification: https://docs.getdbt.com/reference/artifacts/catalog-json
"""

from pprint import pprint

print = pprint
from dataclasses import dataclass, asdict
from json import load


@dataclass(frozen=True)
class Column:
    type_: str
    index: str
    name: str
    comment: str
    description_from_manifest: str = None


@dataclass(frozen=True)
class ManifestData:
    name: str
    columns: list[Column]
    depends_on: list[str]
    raw_code: str
    compiled_code: str
    description: str
    language: str
    tags: list[str]
    package_name: str
    database: str
    schema: str
    materialized: str
    # TODO: store test info for each columns

def format_parsed_data_to_markdown(manifest_data: ManifestData)->str:
    return

def parse_docs_data():
    with open("../dbt_tutorial_w_duckdb/tutorial/target/manifest.json") as f2:
        manifest_json = load(f2)
    with open("../dbt_tutorial_w_duckdb/tutorial/target/catalog.json") as f1:
        catalog_json = load(f1)

    for k, v in catalog_json["nodes"].items():
        # if k.startswith("seed.") or k.startswith("model."):
        if k.startswith("model."):

            manifest_data = manifest_json["nodes"][k]

            columns = []
            for i in v["columns"].values():
                column_info_from_manifest = manifest_data["columns"].get(i["name"])
                if column_info_from_manifest is not None:
                    description_from_manifest = column_info_from_manifest["description"]
                else:
                    description_from_manifest = None

                columns.append(
                    Column(
                        type_=i["type"],
                        index=i["index"],
                        name=i["name"],
                        comment=i["comment"],
                        description_from_manifest=description_from_manifest,
                    )
                )

            d = ManifestData(
                name=k,
                columns=columns,
                raw_code=manifest_data["raw_code"],
                compiled_code=manifest_data["compiled_code"],
                depends_on=manifest_data["depends_on"]["nodes"],
                description=manifest_data["description"],
                language=manifest_data["language"],
                tags=manifest_data["tags"],
                package_name=manifest_data["package_name"],
                database=manifest_data["database"],
                schema=manifest_data["schema"],
                materialized=manifest_data["config"]["materialized"],
            )
            print(asdict(d), sort_dicts=False)

    """
    for k, v in catalog_json["nodes"].items():
        v["columns"]
    """

parse_docs_data()
