HANDOVER — KiDDarn/agentic.github.io

Summary
-------
This handover documents work performed to bootstrap tests and CI, merge updates, remove a committed secret, create an on-demand integration workflow, address test deprecation warnings, add a deterministic dependency lockfile, resolve missing core/integration dependencies, and fix handler coroutine handling. It also lists remaining follow-up actions that require maintainer intervention.

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
13. Synced local repository branch `main` with `origin/main`.
14. Fixed integration testing, missing dependencies, and handler coroutine handling:
   - Added missing `psutil` (7.2.2) to `requirements.txt` (required by `core/monitoring.py`).
   - Added `inspect.iscoroutinefunction` checks to `core/agent_base.py` (`_emit_event`) and `core/api_integration.py` (`WebSocketAPIClient._listen`) to prevent `TypeError` when synchronous handlers are registered.
   - Added UTC timezone fallback for naive ISO timestamp strings in `Message.from_dict` (`core/communication.py`).
   - Refactored `tests/integration/create_sheet_test.py` into a proper pytest integration test (`test_create_and_share_sheet`) with assertion checks, unswallowed exceptions, and graceful skipping when credentials or dependencies are missing.
   - Created `requirements-integration.txt` for `gspread` and `google-auth` and unified all pins in `constraints.txt`.
   - Updated `.github/workflows/integration.yml` to install `requirements-integration.txt -c constraints.txt` and cache dependencies.
   - Enhanced `tests/test_core.py` with coverage for sync/async event handlers, websocket client handlers, and timezone awareness. All 9 tests pass with zero warnings on Python 3.11, 3.12, and 3.14.
15. Audited and expanded unit test suite across all core modules (suite grew from 9 to 43 tests):
   - Added `tests/test_parallel_engine.py`: 10 comprehensive tests for `TaskScheduler` (priority ordering, dependencies resolution), `WorkerPool` (async and thread tasks, unsupported mode errors), and `ParallelExecutionEngine` (async tasks, thread execution, timeouts, exception handling, batch submissions, parallel mapping, pipeline execution, and status reporting).
   - Added `tests/test_swarm_orchestrator.py`: 5 comprehensive tests for `TaskRouter`, `LoadBalancer`, agent registration/duplicates/limits, full agent lifecycle and status queries, and task submission and broadcasting.
   - Added `tests/test_communication.py`: 5 comprehensive tests for `Message` serialization/roundtrip with timezone awareness, `InMemoryMessageBus` handler error isolation and unsubscribe, `CommunicationProtocol` request-response correlation and timeouts, and `CoordinationService` heartbeats, agent status, and task assignments.
   - Added `tests/test_api_integration.py`: 6 comprehensive tests for `RateLimiter` non-deadlocking delay, `HTTPAPIClient` authentication and calling endpoints (success, error response, connection error, unknown endpoints), `GraphQLAPIClient` queries, `APIRegistry`, `APIStackManager`, and `WebSocketAPIClient`.
   - Added `tests/test_monitoring.py`: 5 comprehensive tests for `SystemMetrics`, `MetricsCollector` lifecycle and limits, `CPUAlertRule`, `MemoryAlertRule`, `AgentFailureAlertRule`, `AlertManager` active alerts and resolution, and `MonitoringDashboard` FastAPI routes.
   - Added `tests/test_agent_base.py`: 3 tests for capability and client registration, state transitions (pause/resume/stop), and task execution event emissions (`task_completed`, `task_failed`) with metric updates.
16. Codebase Hardening & Bug Fixes:
   - Fixed fatal deadlock bug in `RateLimiter.acquire()` (`core/api_integration.py`) caused by holding `asyncio.Lock` during recursive calls.
   - Fixed task dependency race condition in `ParallelExecutionEngine._process_tasks()` / `_handle_task_completion()` (`core/parallel_engine.py`) and made thread pool task completions non-blocking using `asyncio.wrap_future()`.
   - Guarded `CommunicationProtocol._setup_default_channels()` (`core/communication.py`) against `RuntimeError` when instantiated outside an active event loop.
   - Replaced bare `except:` clauses with `except Exception:`.
17. Linting and Code Formatting Checks:
   - Added `ruff==0.16.6` to `requirements.txt` and recompiled `constraints.txt` using uv.
   - Created `pyproject.toml` with `ruff` configuration (pycodestyle `E`/`W`, pyflakes `F`, isort `I`, line length 120).
   - Fixed all linting issues across `core/`, `tests/`, and `agentic_os.py`.
   - Added lint and format checks (`ruff check core tests agentic_os.py` and `ruff format --check core tests agentic_os.py`) to `.github/workflows/ci.yml`.
   - Updated `CONTRIBUTING.md` with ruff commands.
18. Hardening & Reviewer Audit Fixes (suite expanded to 54 tests):
   - Fixed `TypeError` in `AgenticOS` instantiation by supporting `max_threads` parameter alias in `ParallelExecutionEngine`.
   - Fixed infinite hang in `SwarmOrchestrator.start_swarm()` by running agent monitoring asynchronously rather than blocking caller execution, and canceling it on `stop_swarm()`.
   - Fixed permanent task loop death on `pause()` in `BaseAgent` by keeping the task loop active in `PAUSED` state and only terminating when `TERMINATED`.
   - Fixed `UnboundLocalError` on cancellation and task ID collision in `ParallelExecutionEngine.pipeline_execution()`, ensuring subsequent pipeline runs do not return stale cached results or mutate caller configurations.
   - Fixed `ExecutionMode.ASYNC` ignoring `timeout` in `submit_task()` by applying `asyncio.wait_for()` and returning `status="timeout"`.
   - Fixed 1.0-second event loop freeze in `MetricsCollector._collect_system_metrics()` by replacing `psutil.cpu_percent(interval=1)` with non-blocking `interval=None`.
   - Fixed race conditions and duplicate subscriptions in `CommunicationProtocol`, made `initialize()` idempotent, and added `cleanup()` and selective `unsubscribe()` by handler.
   - Fixed `WebSocketAPIClient.disconnect()` to cancel the listening task and reset `self.websocket = None`.
   - Replaced deprecated `asyncio.get_event_loop().time()` with `time.time()` in `agentic_os.py`.
   - Added `tests/test_agentic_os.py` (4 tests) covering `AgenticOS` lifecycle, custom agents, task submission, and parallel tasks.
   - Expanded tests across `test_parallel_engine.py`, `test_swarm_orchestrator.py`, `test_communication.py`, and `test_agent_base.py`.
19. Cross-Python CI Matrix & Async Lifecycle Robustness (suite expanded to 56 tests):
   - Added `await asyncio.sleep(0)` to `SwarmOrchestrator.start_agent()` and `start_swarm()` ensuring background agent tasks yield and reach `AgentState.RUNNING` deterministically across Python 3.11 and 3.12.
   - Protected `BaseAgent.start()` against unhandled exceptions during `initialize()`, transitioning to `AgentState.FAILED` and emitting `"agent_failed"`.
   - Set `fail-fast: false` in `.github/workflows/ci.yml` matrix strategy for resilient multi-version test diagnostics.
   - Added unit tests for initialization failures in `test_agent_base.py` and `test_swarm_orchestrator.py` (total 56 passed tests, 0 warnings).
   - Ignored `uv.lock` in `.gitignore`.

Files changed/added
-------------------
- .github/workflows/ci.yml (added ruff lint and format check steps)
- .github/workflows/integration.yml (updated with requirements-integration.txt install and cache)
- requirements.txt (added ruff==0.16.6, psutil==7.2.2, pytest-asyncio==1.4.0)
- requirements-integration.txt (google-auth and gspread)
- constraints.txt (updated lockfile including ruff and all transitive dependencies)
- pyproject.toml (new: ruff configuration)
- pytest.ini (pythonpath, asyncio_mode, and asyncio_default_fixture_loop_scope)
- core/agent_base.py (replaced datetime.utcnow with timezone.utc; supported sync and async event handlers)
- core/api_integration.py (fixed RateLimiter deadlock, replaced bare except, supported sync and async ws handlers)
- core/communication.py (guarded event loop in protocol, replaced datetime.utcnow, normalized naive timestamps)
- core/monitoring.py (replaced bare except, replaced datetime.utcnow and asyncio.iscoroutinefunction)
- core/parallel_engine.py (non-blocking thread futures, fixed dependency completion race condition)
- agentic_os.py (unused variable fixes and formatted)
- tests/test_core.py (unit and async tests for capabilities, tasks, buses, handlers, metrics, and swarm)
- tests/test_async.py (async agent lifecycle tests)
- tests/test_parallel_engine.py (new: comprehensive unit tests for scheduler, pool, and engine)
- tests/test_swarm_orchestrator.py (new: router, balancer, lifecycle, and broadcasting tests)
- tests/test_communication.py (new: protocol, coordination service, request/response, and heartbeat tests)
- tests/test_api_integration.py (new: rate limiter, HTTP client, GraphQL client, registry, and stack manager tests)
- tests/test_monitoring.py (new: metrics collector, alert rules/manager, and FastAPI dashboard endpoints)
- tests/test_agent_base.py (new: capability, state transitions, task execution, and metrics tests)
- tests/test_agentic_os.py (new: AgenticOS lifecycle, custom agent, and parallel execution tests)
- tests/integration/create_sheet_test.py (refactored to formal pytest integration test with noqa)
- CONTRIBUTING.md (updated with constraints.txt, ruff linting, and secret instructions)
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
