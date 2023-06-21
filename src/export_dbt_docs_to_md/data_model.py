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
    owner: str
    package_name: str
    database: str
    schema_: str
    materialized: str


class Test(BaseModel):
    name: str
    materialized: str
    column_name: Optional[str] = Field(...)
    refs: list[str]
    depends_on: list[str]
    raw_code: str
    compiled_code: str
    description: Optional[str] = Field(...)
    test_metadata_name: str

class Macro(BaseModel):
    name: str
    depends_on: dict[str, list[str]]
    arguments: list[str]
    macro_sql: str
    description: Optional[str] = Field(...)
    resource_type: str
    package_name: str

class SourceData(BaseModel):
    name: str
    columns: dict[str, Column]
    description: str
    tags: list[str]
    owner: str
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
    macros: dict[str, Macro]
    parent_map: dict[str, list[str]]
    child_map: dict[str, list[str]]
