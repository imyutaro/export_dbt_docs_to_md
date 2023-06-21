"""
Convert dbt docs(manifest.json and catalog.json) to markdown file.

Inspired by https://github.com/rfdearborn/dbt-docs-to-notion/blob/main/dbt_docs_to_notion.py

manifest.json specification: https://docs.getdbt.com/reference/artifacts/manifest-json
catalog.json specification: https://docs.getdbt.com/reference/artifacts/catalog-json
"""

from json import load
from pathlib import Path
from re import M, sub
from textwrap import dedent
from typing import Optional

from pydantic import BaseModel, Field


class Column(BaseModel):
    type_: str
    index: str
    name: str
    comment: Optional[str] = Field(...)
    description_from_manifest: Optional[str] = Field(...)
    test: set[str] = Field(default_factory=set)


class ManifestData(BaseModel):
    name: str
    columns: dict[str, Column]
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


class Test(BaseModel):
    name: str
    materialized: str
    column_name: str
    refs: list[str]
    depends_on: list[str]
    raw_code: str
    compiled_code: str
    description: Optional[str] = Field(...)
    test_metadata_name: str


class SourceData(BaseModel):
    name: str
    columns: dict[str, Column]
    description: str
    tags: list[str]
    package_name: str
    database: str
    schema_: str
    resource_type: str
    loader: str
    source_name: str


class ParsedJson(BaseModel):
    nodes: dict[str, ManifestData]
    sources: dict[str, SourceData]
    tests: dict[str, Test]
    parent_map: dict[str, list[str]]
    child_map: dict[str, list[str]]


def format_column_info(column_info_list: list[Column]):
    formatted_str = ""
    for column_info in column_info_list.values():
        formatted_str += dedent(
            "| {name} | {type_} | {description} | {test} |\n"
        ).format(
            name=column_info.name,
            type_=column_info.type_,
            description=column_info.description_from_manifest
            if column_info.description_from_manifest is not None
            and column_info.description_from_manifest != ""
            else "&nbsp;",
            test="<br>".join(column_info.test)
            if len(column_info.test) > 0
            else "&nbsp;",
        )
    return formatted_str


def file_path_builder(l: list[str]) -> list[str]:
    """
    replace from . to / for file
    """
    return [i.replace(".", "/") for i in l]


# TODO: separate nodes and source function because nodes and source docs are slightly different format.
def format_parsed_nodes_data_to_markdown(
    name: str, manifest_data: ManifestData, referenced_by: dict[str, str]
) -> str:
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
        description=manifest_data.description
        if manifest_data.description is not None and manifest_data.description != ""
        else "This source is not currently documented",
        formatted_column_info=format_column_info(manifest_data.columns),
        referenced_by_models=referenced_by["models"],
        referenced_by_tests=referenced_by["tests"],
        depends_on="<br>".join(file_path_builder(manifest_data.depends_on)),
        raw_code=manifest_data.raw_code,
        compiled_code=manifest_data.compiled_code,
    )
    output_dir = Path(__file__).parent / "tmp"
    file_path = output_dir / f"{file_path_builder([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)


def format_parsed_source_data_to_markdown(
    name: str, source_data: SourceData, referenced_by: dict[str, str]
) -> str:
    markdown_str = dedent(
        """
    # {table_name}
    {resource_type} table

    ## Details
    | TAGS   | OWNER   | TYPE  | PACKAGE   | RELATION   | LOADER   | SOURCE        |
    | ---    | ---     | ---   | ---       | ---        | ---      | ---           |
    | {tags} | {owner} | table | {package} | {relation} | {loader} | {source_name} |

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

    ## Code
    ### Sample SQL
    ```sql
    select
        {columns}
    from {database}.{schema}.{table_name}
    ```
    """
    ).format(
        table_name=source_data.name,
        resource_type=source_data.resource_type,
        tags=source_data.tags if len(source_data.tags) > 0 else "untagged",
        owner="&nbsp;",
        # type_=source_data.resource_type, # there is no info that is table or view or sth...
        package=source_data.package_name,
        relation=source_data.name,
        loader=source_data.loader
        if source_data.loader is not None and source_data.loader != ""
        else "&nbsp;",
        source_name=source_data.source_name,
        description=source_data.description
        if source_data.description is not None and source_data.description != ""
        else "This source is not currently documented",
        formatted_column_info=format_column_info(source_data.columns),
        referenced_by_models=referenced_by["models"],
        referenced_by_tests=referenced_by["tests"],
        columns=",\n    ".join([col for col in source_data.columns.keys()]),
        database=source_data.database,
        schema=source_data.schema_,
    )
    output_dir = Path(__file__).parent / "tmp"
    file_path = output_dir / f"{file_path_builder([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)


def format_parsed_test_data_to_markdown(name: str, test_data: Test) -> str:
    markdown_str = dedent(
        """
    # {test_name}
    {materialized}

    ## Description
    {description}

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
        test_name=test_data.name,
        materialized=test_data.materialized,
        description=source_data.description
        if test_data.description is None and test_data.description != ""
        else "This test is not currently documented",
        depends_on="<br>".join(file_path_builder(test_data.depends_on)),
        raw_code=dedent(test_data.raw_code),
        compiled_code=sub(r"^\n", "", test_data.compiled_code.strip(), flags=M),
    )
    output_dir = Path(__file__).parent / "tmp"
    file_path = output_dir / f"{file_path_builder([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)


def parse_docs_data():
    with open(
        f"{Path(__file__).parent}/../dbt_tutorial_w_duckdb/tutorial/target/manifest.json"
    ) as f2:
        manifest_json = load(f2)
    with open(
        f"{Path(__file__).parent}/../dbt_tutorial_w_duckdb/tutorial/target/catalog.json"
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
        # if k.startswith("seed.") or k.startswith("model.") or k.startswith("expose."):
        if k.startswith("model."):
            catalog_data = catalog_json[top_level_key][k]

            columns = {}
            for column in catalog_data["columns"].values():
                column_info_from_manifest = manifest_data["columns"].get(column["name"])
                if column_info_from_manifest is not None:
                    description_from_manifest = column_info_from_manifest["description"]
                else:
                    description_from_manifest = None

                columns[column["name"]] = Column(
                    type_=column["type"],
                    index=column["index"],
                    name=column["name"],
                    comment=column["comment"],
                    description_from_manifest=description_from_manifest,
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
                materialized=manifest_data["config"]["materialized"],
                column_name=manifest_data["column_name"],
                refs=manifest_data["refs"][0],
                depends_on=manifest_data["depends_on"]["nodes"],
                raw_code=manifest_data["raw_code"],
                compiled_code=manifest_data["compiled_code"],
                description=manifest_data["description"],
                test_metadata_name=manifest_data["test_metadata"]["name"],
            )
            parsed_json["tests"][k] = t

    top_level_key = "sources"
    for k, manifest_data in manifest_json[top_level_key].items():
        # Store basic info of tables to dataclass.
        if k.startswith("source."):
            catalog_data = catalog_json[top_level_key][k]

            columns = {}
            for column in catalog_data["columns"].values():
                column_info_from_manifest = manifest_data["columns"].get(column["name"])
                if column_info_from_manifest is not None:
                    description_from_manifest = column_info_from_manifest["description"]
                else:
                    description_from_manifest = None

                columns[column["name"]] = Column(
                    type_=column["type"],
                    index=column["index"],
                    name=column["name"],
                    comment=column["comment"],
                    description_from_manifest=description_from_manifest,
                )

            d = SourceData(
                name=manifest_data["name"],
                columns=columns,
                description=manifest_data["description"],
                tags=manifest_data["tags"],
                package_name=manifest_data["package_name"],
                database=manifest_data["database"],
                schema_=manifest_data["schema"],
                resource_type=manifest_data["resource_type"],
                # -- below property exists pnly source node
                loader=manifest_data["loader"],
                source_name=manifest_data["source_name"],
            )

            parsed_json[top_level_key][k] = d

    parsed_json = ParsedJson(
        nodes=parsed_json["nodes"],
        tests=parsed_json["tests"],
        sources=parsed_json["sources"],
        parent_map=manifest_json["parent_map"],
        child_map=manifest_json["child_map"],
    )

    # TODO: deal with other nodes property like seeds.
    for parent_node, child_node_list in parsed_json.child_map.items():
        if not parent_node.startswith("model.") and not parent_node.startswith(
            "source."
        ):
            continue

        referenced_by = {"models": [], "tests": []}
        for child_node in child_node_list:
            if child_node.startswith("model."):
                referenced_by["models"].append(child_node)
            elif child_node.startswith("test."):
                target_col_name = parsed_json.tests[child_node].column_name

                # TODO: Refactor because of non readable and complexity.
                if parent_node.startswith("model."):
                    parsed_json.nodes[parent_node].columns[target_col_name].test.add(
                        parsed_json.tests[child_node].test_metadata_name
                    )
                elif parent_node.startswith("source."):
                    parsed_json.sources[parent_node].columns[target_col_name].test.add(
                        parsed_json.tests[child_node].test_metadata_name
                    )

                # remove test id at the end of child_node name
                test_name_removed_test_id = ".".join(child_node.split(".")[:-1])
                format_parsed_test_data_to_markdown(
                    test_name_removed_test_id,
                    parsed_json.tests[child_node],
                )
                referenced_by["tests"].append(test_name_removed_test_id)

        # format reference info
        for reference_by_key, reference_by_val in referenced_by.items():
            referenced_by[reference_by_key] = (
                "<br>".join(file_path_builder(referenced_by[reference_by_key]))
                if len(referenced_by[reference_by_key]) > 0
                else "&nbsp;"
            )

        if parent_node.startswith("model."):
            node_data = parsed_json.nodes[parent_node]
            format_parsed_nodes_data_to_markdown(parent_node, node_data, referenced_by)
        if parent_node.startswith("source."):
            node_data = parsed_json.sources[parent_node]
            format_parsed_source_data_to_markdown(parent_node, node_data, referenced_by)


parse_docs_data()
