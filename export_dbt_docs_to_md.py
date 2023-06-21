"""
Convert dbt docs(manifest.json and catalog.json) to markdown file.

Inspired by https://github.com/rfdearborn/dbt-docs-to-notion/blob/main/dbt_docs_to_notion.py

manifest.json specification: https://docs.getdbt.com/reference/artifacts/manifest-json
catalog.json specification: https://docs.getdbt.com/reference/artifacts/catalog-json
"""

from pathlib import Path
from pprint import pprint

print = pprint
import os
from json import load
from textwrap import dedent
from typing import Optional

from pydantic import BaseModel, Field


class Column(BaseModel):
    type_: str
    index: str
    name: str
    comment: Optional[str] = Field(...)
    description_from_manifest: Optional[str] = Field(...)
    # test: Optional[set[str]] = Field(...)


class ManifestData(BaseModel):
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
    schema_: str
    materialized: str
    # TODO: store test info for each columns


class Test(BaseModel):
    name: str
    column_name: str
    refs: list[str]
    raw_code: str
    compiled_code: str
    description: Optional[str] = Field(...)

class SourceData(BaseModel):
    name: str
    columns: list[Column]
    description: str
    tags: list[str]
    package_name: str
    database: str
    schema_: str
    resource_type: str
    loader: str
    source: str

class ParsedJson(BaseModel):
    nodes: dict[str, ManifestData]
    tests: dict[str, Test]
    parent_map: dict[str, list[str]]
    child_map: dict[str, list[str]]


def format_column_info(column_info_list: list[Column]):
    formatted_str = ""
    for column_info in column_info_list:
        formatted_str += dedent(
            f" | {column_info.name}"
            f" | {column_info.type_}"
            f" | {column_info.description_from_manifest}"
            f" | &nbsp; |\n"
        )
    return formatted_str


def file_path_builder(l: list[str]) -> list[str]:
    """
    replace from . to / for file
    """
    return [i.replace(".","/") for i in l]


# TODO: nodesとsourceでdocsのフォーマットが微妙に違うので、関数を分けたほうがいいかも
def format_parsed_data_to_markdown(name: str, manifest_data: ManifestData, referenced_by:dict[str,list[str]]) -> str:
    print(manifest_data.dict(), sort_dicts=False)
    markdown_str = dedent(
        """
    # {table_name}
    {materialized}

    ## Details
    | TAGS   | OWNER   | TYPE    | PACKAGE   | LANGUAGE   | RELATION   |
    | ---    | ---     | ---     | ---       | ---        | ---        |
    | {tags} | {owner} | {type_} | {package} | {language} | {relation} |

    ## Description
    {description}

    ## Columns
    | COLUMN | TYPE | DESCRIPTION | TESTS |
    | ---    | ---  | ---         | ---   |
    {formatted_column_info}

    ## Referenced By
    | Models                 | Tests                 |
    | ---                    | ---                   |
    | {referenced_by_models} | {referenced_by_tests} |

    ## Depends On
    | Models |
    | ---    |
    | {depends_on} |

    ## Code
    ### Source
    ```sql
    {raw_code}
    ```
    ### Compiled
    ```sql
    {compiled_code}
    ```
    """
    ).format(
        table_name=manifest_data.name,
        materialized=manifest_data.materialized,
        tags=manifest_data.tags if len(manifest_data.tags) > 0 else "untagged",
        owner="&nbsp;",
        type_=manifest_data.materialized,
        package=manifest_data.package_name,
        language=manifest_data.language,
        relation=manifest_data.name,
        description=manifest_data.description,
        formatted_column_info=format_column_info(manifest_data.columns),
        referenced_by_models="<br>".join(file_path_builder(referenced_by["models"])),
        referenced_by_tests="<br>".join(file_path_builder(referenced_by["tests"])),
        depends_on="<br>".join(file_path_builder(manifest_data.depends_on)),
        raw_code=manifest_data.raw_code,
        compiled_code=manifest_data.compiled_code,
    )
    print(markdown_str)
    output_dir = Path(__file__).parent / "tmp"
    file_path = output_dir / f"{file_path_builder([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)

def store_data(key: str):
    return

def parse_docs_data():
    with open(
        f"{os.path.dirname(__file__)}/../dbt_tutorial_w_duckdb/tutorial/target/manifest.json"
    ) as f2:
        manifest_json = load(f2)
    with open(
        f"{os.path.dirname(__file__)}/../dbt_tutorial_w_duckdb/tutorial/target/catalog.json"
    ) as f1:
        catalog_json = load(f1)

    parsed_json = {
        "nodes": {},
        "tests": {},
        "sources": {},
        "parent_map": {},
        "child_map": {},
    }
    top_level_key = "nodes"
    for k, manifest_data in manifest_json[top_level_key].items():
        # Store basic info of tables to dataclass.
        # if k.startswith("seed.") or k.startswith("model.") or k.startswith("source.") or k.startswith("expose."):
        if k.startswith("model."):
            catalog_data = catalog_json[top_level_key][k]

            columns = []
            for column in catalog_data["columns"].values():
                column_info_from_manifest = manifest_data["columns"].get(column["name"])
                if column_info_from_manifest is not None:
                    description_from_manifest = column_info_from_manifest["description"]
                else:
                    description_from_manifest = None

                columns.append(
                    Column(
                        type_=column["type"],
                        index=column["index"],
                        name=column["name"],
                        comment=column["comment"],
                        description_from_manifest=description_from_manifest,
                    )
                )

            d = ManifestData(
                name=manifest_data["name"],
                columns=columns,
                raw_code=manifest_data["raw_code"],
                compiled_code=manifest_data["compiled_code"],
                depends_on=manifest_data["depends_on"]["nodes"],
                description=manifest_data["description"],
                language=manifest_data["language"],
                tags=manifest_data["tags"],
                package_name=manifest_data["package_name"],
                database=manifest_data["database"],
                schema_=manifest_data["schema"],
                materialized=manifest_data["config"]["materialized"],
                resource_type=manifest_data["resource_type"],
            )

            parsed_json[top_level_key][k] = d

        elif k.startswith("test."):
            t = Test(
                name=manifest_data["name"],
                column_name=manifest_data["column_name"],
                refs=manifest_data["refs"][0],
                raw_code=manifest_data["raw_code"],
                compiled_code=manifest_data["compiled_code"],
                description=manifest_data["description"],
            )
            parsed_json["tests"][k] = t


    # top_level_key = "sources"
    # for k, manifest_data in manifest_json[top_level_key].items():
    #     # Store basic info of tables to dataclass.
    #     if k.startswith("source."):
    #         catalog_data = catalog_json[top_level_key][k]

    #         columns = []
    #         for column in catalog_data["columns"].values():
    #             column_info_from_manifest = manifest_data["columns"].get(column["name"])
    #             if column_info_from_manifest is not None:
    #                 description_from_manifest = column_info_from_manifest["description"]
    #             else:
    #                 description_from_manifest = None

    #             columns.append(
    #                 Column(
    #                     type_=column["type"],
    #                     index=column["index"],
    #                     name=column["name"],
    #                     comment=column["comment"],
    #                     description_from_manifest=description_from_manifest,
    #                 )
    #             )

    #         d = SourceData(
    #             name=manifest_data["name"],
    #             columns=columns,
    #             description=manifest_data["description"],
    #             tags=manifest_data["tags"],
    #             package_name=manifest_data["package_name"],
    #             database=manifest_data["database"],
    #             schema_=manifest_data["schema"],
    #             resource_type=manifest_data["resource_type"],
    #             # -- below property exists pnly source node
    #             loader=manifest_data["loader"],
    #             source=manifest_data["source_name"]
    #         )

    #         parsed_json[top_level_key][k] = d

    parsed_json = ParsedJson(
        nodes=parsed_json["nodes"],
        tests=parsed_json["tests"],
        # sources=parsed_json["sources"],
        parent_map=manifest_json["parent_map"],
        child_map=manifest_json["child_map"],
    )

    # TODO: use child_map or parent_map in manifest.json to relate tables to tests.
    for parent_node, child_node_list in parsed_json.child_map.items():
        # if not parent_node.startswith("model.") and not parent_node.startswith("source."):
        if not parent_node.startswith("model."):
            continue

        node_data = parsed_json.nodes[parent_node]
        referenced_by = {"models":[], "tests":[]}
        for child_node in child_node_list:
            # if child_node.startswith("model.") or child_node.startswith("source."):
            if child_node.startswith("model."):
                referenced_by["models"].append(child_node)
            elif child_node.startswith("test."):
                # remove test id at the end of child_node name
                referenced_by["tests"].append(".".join(child_node.split(".")[:-1]))

        format_parsed_data_to_markdown(parent_node, node_data, referenced_by)


parse_docs_data()
