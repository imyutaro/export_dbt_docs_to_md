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
    name: str
    # type_: str = None
    description: str
    tags: list[str]


@dataclass(frozen=True)
class ManifestData:
    name: str
    columns: list[Column]
    depends_on: list[str]
    raw_code: str
    compiled_code: str
    description: str
    language: str
    # TODO: store test info for each columns


def main():
    with open("../dbt_tutorial_w_duckdb/tutorial/target/manifest.json") as f2:
        manifest_json = load(f2)
    with open("../dbt_tutorial_w_duckdb/tutorial/target/catalog.json") as f1:
        catalog_json = load(f1)

    for k, v in manifest_json["nodes"].items():
        # if k.startswith("seed.") or k.startswith("model."):
        if k.startswith("model."):
            # print("")
            # print(k, sort_dicts=False)
            # print(v, sort_dicts=False)
            columns = []
            for i in v["columns"].values():
                columns.append(
                    Column(
                        name=i["name"],
                        description=i["description"],
                        tags=i["tags"],
                    )
                )
            d = ManifestData(
                name=k,
                columns=columns,
                raw_code=v["raw_code"],
                compiled_code=v["compiled_code"],
                depends_on=v["depends_on"]["nodes"],
                description=v["description"],
                language=v["language"]
            )
            # print(asdict(d), sort_dicts=False)

    for k, v in catalog_json["nodes"].items():

main()
