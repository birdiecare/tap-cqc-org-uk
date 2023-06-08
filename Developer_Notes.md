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
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-cqc-org-uk
meltano install
```

Now you can test and orchestrate using Meltano:
```bash
# Test invocation:
meltano invoke tap-cqc-org-uk --version
# OR run a test `elt` pipeline:
meltano elt tap-cqc-org-uk target-jsonl
```