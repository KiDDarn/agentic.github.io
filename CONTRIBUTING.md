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