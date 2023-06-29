from pathlib import Path
from re import M, sub
from textwrap import dedent

from data_model import Column, Macro, ManifestData, SourceData, Test


def model2filepath(data_model_list: list[str]) -> list[str]:
    """
    replace from . to / for file
    """
    if type(data_model_list) is not list:
        raise TypeError(f"Invalid type: {type(data_model_list)}")
    if len(data_model_list) == 0:
        return ["&nbsp;"]

    return [i.replace(".", "/") for i in data_model_list]


def format_column_info(column_info_list: list[Column]):
    formatted_str = ""
    for column_info in column_info_list.values():
        formatted_str += dedent("| {name} | {type_} | {description} | {test} |\n").format(
            name=column_info.name,
            type_=column_info.type_,
            description=column_info.description_from_manifest
            if column_info.description_from_manifest is not None and column_info.description_from_manifest != ""
            else "&nbsp;",
            test="<br>".join(column_info.test) if len(column_info.test) > 0 else "&nbsp;",
        )
    return formatted_str


# TODO: separate nodes and source function because nodes and source docs are slightly different format.
def format_models_data(
    name: str, manifest_data: ManifestData, referenced_by: dict[str, str], output_dir_path: str | Path
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
        owner=manifest_data.owner,
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
        depends_on="<br>".join(model2filepath(manifest_data.depends_on)),
        raw_code=manifest_data.raw_code,
        compiled_code=sub(r"^\n", "", manifest_data.compiled_code.strip(), flags=M),
    )

    output_dir_path = output_dir_path / manifest_data.package_name
    file_path = output_dir_path / f"{model2filepath([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)


def format_seeds_data(
    name: str, manifest_data: ManifestData, referenced_by: dict[str, str], output_dir_path: str | Path
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

    ## Code
    ### Example SQL
    ```sql
    select
        {columns}
    from {database}.{schema}.{table_name}
    ```
    """
    ).format(
        table_name=manifest_data.name,
        materialized=manifest_data.materialized,
        tags=manifest_data.tags if len(manifest_data.tags) > 0 else "untagged",
        owner=manifest_data.owner,
        type_=manifest_data.materialized,
        package=manifest_data.package_name,
        language=manifest_data.language,
        relation=manifest_data.name,
        description=manifest_data.description
        if manifest_data.description is not None and manifest_data.description != ""
        else "This seed is not currently documented",
        formatted_column_info=format_column_info(manifest_data.columns),
        columns=",\n    ".join([col for col in manifest_data.columns.keys()]),
        database=manifest_data.database,
        schema=manifest_data.schema_,
    )

    output_dir_path = Path(__file__).parent / "tmp" / manifest_data.package_name
    file_path = output_dir_path / f"{model2filepath([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)


def format_source_data(
    name: str, source_data: SourceData, referenced_by: dict[str, str], output_dir_path: str | Path
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
        owner=source_data.owner,
        # type_=source_data.resource_type, # there is no info that is table or view or sth...
        package=source_data.package_name,
        relation=source_data.name,
        loader=source_data.loader if source_data.loader is not None and source_data.loader != "" else "&nbsp;",
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

    output_dir_path = output_dir_path / source_data.package_name
    file_path = output_dir_path / f"{model2filepath([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)


def format_test_data(name: str, test_data: Test, output_dir_path: str | Path) -> str:
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
        description=test_data.description
        if test_data.description is None and test_data.description != ""
        else "This test is not currently documented",
        depends_on="<br>".join(model2filepath(test_data.depends_on)),
        raw_code=dedent(test_data.raw_code),
        compiled_code=sub(r"^\n", "", test_data.compiled_code.strip(), flags=M),
    )

    output_dir_path = output_dir_path / "tests"
    file_path = output_dir_path / f"{model2filepath([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)


def format_macro_data(name: str, macro_data: Macro, output_dir_path: str | Path) -> str:
    markdown_str = dedent(
        """
    # {macro_name}
    {resource_type}

    ## Description
    {description}

    ## Arguments
    {arguments}

    ## Referenced By
    | Models |
    | ---    |
    | {referenced_by} |

    ## Depends On
    | Models              | Macros              |
    | ---                 | ---                 |
    | {depends_on_models} | {depends_on_macros} |

    ## Code
    ### Source
    ```sql
    {macro_sql}
    ```
    """
    ).format(
        macro_name=macro_data.name,
        resource_type=macro_data.resource_type,
        description=macro_data.description
        if macro_data.description is None and macro_data.description != ""
        else "This macro is not currently documented",
        arguments=macro_data.arguments
        if macro_data.arguments is None and macro_data.arguments != ""
        else "Details are not available for this macro",
        # referenced_by="<br>".join(model2filepath(macro_data.referenced_by)),
        referenced_by="&nbsp;",
        depends_on_models="<br>".join(model2filepath(macro_data.depends_on.get("models", ["&nbsp;"]))),
        depends_on_macros="<br>".join(model2filepath(macro_data.depends_on.get("macros", ["&nbsp;"]))),
        macro_sql=dedent(macro_data.macro_sql),
    )

    output_dir_path = output_dir_path / macro_data.package_name
    file_path = output_dir_path / f"{model2filepath([name])[0]}.md"
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with open(file_path, mode="w") as f:
        f.write(markdown_str)
