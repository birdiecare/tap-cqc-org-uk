### Initialize your Development Environment

```bash
pipx install singer_sdk
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_cqc_org_uk/tests` subfolder and then run:
`poetry run pytest`

You can also test the `tap-cqc-org-uk` CLI interface directly using `poetry run`:
`poetry run tap-cqc-org-uk --config config_example.json`


Next, install Meltano (if you haven't already) and any needed plugins:
```bash
cd tap-cqc-org-uk
# Initialize meltano within this directory
poetry run meltano install
poetry run meltano add extractor tap-cqc-org-uk
poetry run meltano install loader target-jsonl
```

Make sure that you have a `meltano.yml` file in your root project folder like this:
```YAML
  version: 1
  send_anonymous_usage_stats: false
  default_environment: dev
  project_id: a2afb24d-9202-4a61-bad9-ab28d2396999
  environments:
  - name: dev
  plugins:
    extractors:
    # Using tap-cqc-org-uk v0.0.3
    - name: tap-cqc-org-uk
      load_schema: REGULATOR_DATA
      namespace: tap_cqc_org_uk
      pip_url: -e .
      executable: tap-cqc-org-uk
      capabilities:
      - catalog
      - state
      - discover
      settings:
      - name: start_date
        value: '2021-12-01T00:00:00Z'
      config:
        start_date: $CQC_ORG_UK_TO_SNOWFLAKE_START_DATE
        partner_code: Birdie Care
    loaders:
    - name: target-jsonl
      variant: andyh1203
      pip_url: target-jsonl
```

Now you can test and orchestrate using Meltano:
```bash
# Test invocation:
poetry run meltano invoke tap-cqc-org-uk --version
# OR run a test `elt` pipeline:
export CQC_ORG_UK_TO_SNOWFLAKE_START_DATE='2021-12-01T00:00:00Z'
poetry run meltano elt tap-cqc-org-uk target-jsonl
```