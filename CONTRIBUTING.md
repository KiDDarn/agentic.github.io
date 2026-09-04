# Contributing

Thanks for contributing! Quick guide to run tests locally and set up a development environment.

1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows (PowerShell/CMD)
```

2. Install dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

3. Run the test suite

```bash
# Run only the unit tests discovered under tests/
PYTHONPATH=. pytest -q

# Run an individual test file
PYTHONPATH=. pytest tests/test_core.py -q
```

4. Async tests

This project uses pytest-asyncio for async tests. The installed requirements include pytest-asyncio; tests marked with `@pytest.mark.asyncio` will run normally.

5. Adding tests

- Put unit tests in `tests/` (they run by default).
- Put integration or long-running tests under `tests/integration/` to avoid running them in CI by default.

6. Running CI locally

You can run the same matrix locally using tox or by running multiple interpreters. The CI caches pip packages to speed up workflow runs.

Questions? Open an issue or PR with details.

## Secrets & Integration tests

IMPORTANT: A service account JSON file (service_account.json) was previously committed and has been removed from the repository for security. The file must be rotated immediately and never re-committed.

To run integration tests (on-demand) a repository secret named `SERVICE_ACCOUNT_JSON` must be added with the full contents of the service account JSON file. The project includes an on-demand workflow `.github/workflows/integration.yml` that will:

- Restore the secret into `service_account.json` at workflow runtime
- Install dependencies
- Run pytest only for `tests/integration`

How to add the secret:
1. Go to the repository Settings → Secrets and variables → Actions → New repository secret
2. Name: `SERVICE_ACCOUNT_JSON`
3. Paste the JSON file contents as the secret value

How to run integration tests via Actions:
1. In GitHub, open the repository Actions tab → Integration tests workflow
2. Click "Run workflow" and confirm

Do NOT commit credentials or private keys into the repository. If you discover a committed secret, rotate it immediately and notify the maintainers.