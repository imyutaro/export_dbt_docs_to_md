How to use
---

```
$ python src/export_dbt_docs_to_md/export_dbt_docs_to_md.py --help
usage: export_dbt_docs_to_md.py [-h] [-m MANIFEST_JSON] [-c CATALOG_JSON] [-o OUTPUT_DIR]

options:
  -h, --help            show this help message and exit
  -m MANIFEST_JSON, --manifest_json MANIFEST_JSON
  -c CATALOG_JSON, --catalog_json CATALOG_JSON
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR

$ python src/export_dbt_docs_to_md/export_dbt_docs_to_md.py --manifest_json=tmp_dummy_data/manifest_facebook.json --catalog_json=tmp_dummy_data/catalog_facebook.json --output_dir=output_dir
```

TODO
---

- [ ] put explanation for output dir structure in README or/and args help str.
- [ ] write test
  - check how dbt-docs do CI/testing
  - https://github.com/dbt-labs/dbt-docs/tree/main/data
  - https://github.com/fivetran/dbt_facebook_ads/tree/main/docs
- [ ] export_dbt_docs_to_md.py:96:  # TODO: Currently, I cannot find a way to extract referenced_by info from manifest.json and catalog.json
- [ ] export_dbt_docs_to_md.py:163: # TODO: deal with other nodes property analysis, exposure, metric and docs.
- [ ] export_dbt_docs_to_md.py:181: # TODO: Refactor. This is too complex to understand what is doing here.
- [ ] format_parsed_data.py:34:     # TODO: separate nodes and source function because nodes and source docs are slightly different format.

