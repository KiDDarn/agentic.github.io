HANDOVER — KiDDarn/agentic.github.io

Summary
-------
This handover documents work performed to bootstrap tests and CI, merge updates, remove a committed secret, create an on-demand integration workflow, address test deprecation warnings, and add a deterministic dependency lockfile. It also lists remaining follow-up actions that require maintainer intervention.

What was done
-------------
1. Codebase exploration and architecture summary (previous work).
2. Added minimal unit + async tests under tests/ and moved integration tests to tests/integration/.
3. Created initial CI workflow: .github/workflows/ci.yml — matrix for Python (3.11, 3.12), pip cache, sets PYTHONPATH in job env, runs pytest.
4. Iteratively fixed CI issues:
   - Resolved matrix parsing quirks
   - Set PYTHONPATH to ensure tests import local packages
   - Excluded integration tests from default discovery with pytest.ini
   - Pinned dependencies in requirements.txt and added pytest-asyncio
5. Merged origin/main into branch kiddarn-urban-lamp and resolved a conflict in .github/workflows/ci.yml (kept matrix + cache + PYTHONPATH env).
6. Fixed syntax/indentation issue in implementation/klap_generate_shorts_enhanced.py that broke local compile checks.
7. Created on-demand integration workflow: .github/workflows/integration.yml. It restores SERVICE_ACCOUNT_JSON secret into service_account.json and runs pytest only on tests/integration.
8. Removed committed service_account.json from the repository, added service_account.json to .gitignore.
9. Updated CONTRIBUTING.md with a "Secrets & Integration tests" section explaining rotation and secret setup.
10. Opened PR #1 for the merge & changes; merged PR #1 into main and deleted the branch.
11. Addressed deprecation warnings:
   - Updated `pytest-asyncio` from 0.21.0 to 1.4.0 (resolved 120+ `asyncio.iscoroutinefunction` and `asyncio.get_event_loop_policy` deprecation warnings on Python 3.12+).
   - Replaced `datetime.utcnow()` with `datetime.now(timezone.utc)` across `core/agent_base.py`, `core/api_integration.py`, `core/communication.py`, and `core/monitoring.py`.
   - Replaced deprecated `asyncio.iscoroutinefunction()` with standard `inspect.iscoroutinefunction()` in `core/communication.py` and `core/monitoring.py`.
   - Added `pythonpath = .`, `asyncio_mode = auto`, and `asyncio_default_fixture_loop_scope = function` to `pytest.ini` for seamless local test execution.
12. Added deterministic dependency lockfile (`constraints.txt`):
   - Generated pinned transitive dependencies using uv/pip.
   - Updated `.github/workflows/ci.yml` and `.github/workflows/integration.yml` to install with `-c constraints.txt` and cache based on `constraints.txt`.
   - Updated `CONTRIBUTING.md` with instructions for using `constraints.txt`.
13. Synced local repository branch `main` with `origin/main` and verified test suite runs cleanly (4 passed, 0 warnings).

Files changed/added
-------------------
- .github/workflows/ci.yml (updated with constraints.txt support and cache key)
- .github/workflows/integration.yml (updated with constraints.txt support)
- requirements.txt (updated pytest-asyncio to 1.4.0)
- constraints.txt (new: deterministic lockfile)
- pytest.ini (added pythonpath, asyncio_mode, and asyncio_default_fixture_loop_scope)
- core/agent_base.py (replaced datetime.utcnow with timezone.utc)
- core/api_integration.py (replaced datetime.utcnow with timezone.utc)
- core/communication.py (replaced datetime.utcnow and asyncio.iscoroutinefunction)
- core/monitoring.py (replaced datetime.utcnow and asyncio.iscoroutinefunction)
- tests/test_core.py (unit tests)
- tests/test_async.py (async tests)
- tests/integration/create_sheet_test.py (moved)
- CONTRIBUTING.md (updated with constraints.txt and secret instructions)
- .gitignore (added service_account.json and .venv/)
- HANDOVER.md (this file)

Security & secrets
------------------
- A service account JSON (service_account.json) was previously committed and has been removed from the repo and ignored. This credential must be rotated immediately by a maintainer with GCP access.
- On-demand integration workflow expects a repository secret: SERVICE_ACCOUNT_JSON (the JSON file contents) to run integration tests safely.

How to rotate credentials (maintainer action)
--------------------------------------------
1. Revoke compromised key(s) in Google Cloud:
   gcloud iam service-accounts keys list --iam-account=SERVICE_ACCOUNT_EMAIL
   gcloud iam service-accounts keys delete KEY_ID --iam-account=SERVICE_ACCOUNT_EMAIL
2. Create a new key and download JSON:
   gcloud iam service-accounts keys create new-key.json --iam-account=SERVICE_ACCOUNT_EMAIL
3. Add secret in GitHub:
   gh secret set SERVICE_ACCOUNT_JSON --body "$(cat new-key.json)" --repo KiDDarn/agentic.github.io

Running integration tests (after secret added)
----------------------------------------------
- Use GitHub Actions → Integration tests → Run workflow, or run:
  gh workflow run integration.yml --repo KiDDarn/agentic.github.io

Pending follow-ups (maintainer / external actions)
--------------------------------------------------
- Rotate/revoke the exposed service account key in Google Cloud IAM (security).
- Set the `SERVICE_ACCOUNT_JSON` repository secret in GitHub repo settings (admin permission required).
- Trigger the on-demand integration workflow (`gh workflow run integration.yml` or GitHub Actions UI) once the secret is set, and address any integration-specific failures.

End of handover.
