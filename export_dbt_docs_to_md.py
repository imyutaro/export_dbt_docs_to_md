"""
Convert dbt docs(manifest.json and catalog.json) to markdown file.

manifest.json specification: https://docs.getdbt.com/reference/artifacts/manifest-json
catalog.json specification: https://docs.getdbt.com/reference/artifacts/catalog-json
"""

from pprint import pprint
print=pprint
from json import load
from dataclasses import dataclass


@dataclass(frozen=True)
class Details:
    tags: str
    owner: str
    type_: str
    package: str
    language: str
    relation: str

@dataclass(frozen=True)
class Column:
    name: str
    type_: str
    descroptoin: str


@dataclass(frozen=True)
class Table:
    name: str
    aaaaa: str                  # TODO: check specification
    details: Details
    descroptoin: str
    columns: list[Column]
    referenced_by:  list[str]   # TODO: check specification
    depends_on: str
    raw_code: str
    compiled_code: str


def main():
    with open("../dbt_tutorial_w_duckdb/tutorial/target/manifest.json") as f2:
        manifest_json = load(f2)
    with open("../dbt_tutorial_w_duckdb/tutorial/target/catalog.json") as f1:
        catalog_json = load(f1)

    print(manifest_json["nodes"]["model.tutorial.customers"]["columns"])
    # print(catalog_json)



main()

