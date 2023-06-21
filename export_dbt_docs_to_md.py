"""
Convert dbt docs(manifest.json and catalog.json) to markdown file.

Inspired by https://github.com/rfdearborn/dbt-docs-to-notion/blob/main/dbt_docs_to_notion.py

manifest.json specification: https://docs.getdbt.com/reference/artifacts/manifest-json
catalog.json specification: https://docs.getdbt.com/reference/artifacts/catalog-json
"""

from json import load
from pathlib import Path

from data_model import Column, Macro, ManifestData, ParsedJson, SourceData, Test
from format_parsed_data import (
    format_macro_data,
    format_models_data,
    format_seeds_data,
    format_source_data,
    format_test_data,
    model2filepath,
)


def parse_docs_data():
    # TODO: specify input dir from cmd line.
    with open(
        # f"{Path(__file__).parent}/../dbt_tutorial_w_duckdb/tutorial/target/manifest.json"
        f"{Path(__file__).parent}/tmp_dummy_data/manifest_facebook.json"
    ) as f2:
        manifest_json = load(f2)
    with open(
        # f"{Path(__file__).parent}/../dbt_tutorial_w_duckdb/tutorial/target/catalog.json"
        f"{Path(__file__).parent}/tmp_dummy_data/catalog_facebook.json"
    ) as f1:
        catalog_json = load(f1)

    parsed_json_dict = {
        "nodes": {},
        "tests": {},
        "macros": {},
        "sources": {},
        "parent_map": {},
        "child_map": {},
    }
    top_level_key = "nodes"
    for k, manifest_data in manifest_json[top_level_key].items():
        # Store basic info of tables to dataclass.
        if k.startswith("model.") or k.startswith("seed."):
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
                owner=catalog_data["metadata"]["owner"],
                package_name=manifest_data["package_name"],
                database=manifest_data["database"],
                schema_=manifest_data["schema"],
                materialized=manifest_data["config"]["materialized"],
                resource_type=manifest_data["resource_type"],
            )

            parsed_json_dict[top_level_key][k] = d

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
            parsed_json_dict["tests"][k] = t

    top_level_key = "macros"
    for k, manifest_data in manifest_json[top_level_key].items():
        # TODO: Currently, I cannot find a way to extract referenced_by info from manifest.json and catalog.json
        if k.startswith("macro.dbt."):
            # dbt-docs ignores dbt default macro, ignore here too.
            continue
        elif k.startswith("macro."):
            m = Macro(
                name=manifest_data["name"],
                depends_on=manifest_data["depends_on"],
                arguments=manifest_data["arguments"],
                macro_sql=manifest_data["macro_sql"],
                description=manifest_data["description"],
                resource_type=manifest_data["resource_type"],
                package_name=manifest_data["package_name"],
            )
            parsed_json_dict["macros"][k] = m

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
                owner=catalog_data["metadata"]["owner"],
                package_name=manifest_data["package_name"],
                database=manifest_data["database"],
                schema_=manifest_data["schema"],
                resource_type=manifest_data["resource_type"],
                # -- below property exists pnly source node
                loader=manifest_data["loader"],
                source_name=manifest_data["source_name"],
            )

            parsed_json_dict[top_level_key][k] = d

    parsed_json = ParsedJson(
        nodes=parsed_json_dict["nodes"],
        tests=parsed_json_dict["tests"],
        macros=parsed_json_dict["macros"],
        sources=parsed_json_dict["sources"],
        parent_map=manifest_json["parent_map"],
        child_map=manifest_json["child_map"],
    )

    for name, macro_data in parsed_json.macros.items():
        format_macro_data(
            name=name,
            macro_data=macro_data,
        )

    # TODO: deal with other nodes property analysis, exposure, metric and docs.
    for parent_node, child_node_list in parsed_json.child_map.items():
        if (
            not parent_node.startswith("model.")
            and not parent_node.startswith("source.")
            and not parent_node.startswith("seed.")
        ):
            continue

        referenced_by = {"models": [], "tests": []}
        for child_node in child_node_list:
            if child_node.startswith("model.") or child_node.startswith("seed."):
                referenced_by["models"].append(child_node)
            elif child_node.startswith("test."):
                target_col_name = parsed_json.tests[child_node].column_name
                if target_col_name is None:
                    continue

                # TODO: Refactor. This is too complex to understand what it's doing here.
                #       This process is "Store relationship between tests and tables".
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
                format_test_data(
                    test_name_removed_test_id,
                    parsed_json.tests[child_node],
                )
                referenced_by["tests"].append(test_name_removed_test_id)

        # format reference info
        for reference_by_key in referenced_by.keys():
            referenced_by[reference_by_key] = (
                "<br>".join(model2filepath(referenced_by[reference_by_key]))
                if len(referenced_by[reference_by_key]) > 0
                else "&nbsp;"
            )

        if parent_node.startswith("model."):
            node_data = parsed_json.nodes[parent_node]
            format_models_data(parent_node, node_data, referenced_by)
        if parent_node.startswith("seed."):
            node_data = parsed_json.nodes[parent_node]
            format_seeds_data(parent_node, node_data, referenced_by)
        if parent_node.startswith("source."):
            node_data = parsed_json.sources[parent_node]
            format_source_data(parent_node, node_data, referenced_by)


parse_docs_data()
