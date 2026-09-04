import asyncio

import pytest

from core.parallel_engine import (
    ExecutionMode,
    ExecutionResult,
    ExecutionTask,
    ParallelExecutionEngine,
    TaskScheduler,
    WorkerPool,
)


def _process_worker_cube(x: int) -> int:
    return x * x * x


def test_execution_task_defaults():
    task = ExecutionTask(id="task-1", func=lambda: None)
    assert task.id == "task-1"
    assert task.args == ()
    assert task.kwargs == {}
    assert task.dependencies == []
    assert task.priority == 0
    assert task.mode == ExecutionMode.ASYNC


def test_execution_result_defaults():
    res = ExecutionResult(task_id="res-1")
    assert res.task_id == "res-1"
    assert res.result is None
    assert res.error is None
    assert res.execution_time == 0.0
    assert res.status == "pending"


def test_task_scheduler_priority_and_dependencies():
    scheduler = TaskScheduler()

    # t1 has priority 10, t2 has priority 1
    t1 = ExecutionTask(id="t1", func=lambda: None, priority=10)
    t2 = ExecutionTask(id="t2", func=lambda: None, priority=1)
    # t3 depends on t1
    t3 = ExecutionTask(id="t3", func=lambda: None, priority=0, dependencies=["t1"])

    scheduler.add_task(t1)
    scheduler.add_task(t2)
    scheduler.add_task(t3)

    # t2 should be popped before t1 due to lower priority number
    ready1 = scheduler.get_ready_task()
    assert ready1 is not None
    assert ready1.id == "t2"

    # Next ready task should be t1
    ready2 = scheduler.get_ready_task()
    assert ready2 is not None
    assert ready2.id == "t1"

    # t3 is waiting on t1, so no tasks should be ready now
    assert scheduler.get_ready_task() is None

    # Completing t1 should unblock t3
    res_t1 = ExecutionResult(task_id="t1", status="completed")
    scheduler.mark_task_completed("t1", res_t1)
    assert scheduler.get_completed_result("t1") == res_t1

    ready3 = scheduler.get_ready_task()
    assert ready3 is not None
    assert ready3.id == "t3"


@pytest.mark.asyncio
async def test_worker_pool_unsupported_mode():
    pool = WorkerPool(thread_workers=2, process_workers=2)
    task = ExecutionTask(id="t-err", func=lambda: None, mode="unsupported")  # type: ignore
    with pytest.raises(ValueError, match="Unsupported execution mode"):
        await pool.submit_task(task)
    await pool.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_async_task():
    engine = ParallelExecutionEngine(max_concurrent_tasks=5)

    async def add(a, b):
        await asyncio.sleep(0.01)
        return a + b

    task_id = await engine.submit_task(func=add, args=(10, 20), mode=ExecutionMode.ASYNC)
    result = await engine.wait_for_task(task_id, timeout=2.0)

    assert result.status == "completed"
    assert result.result == 30
    assert result.error is None
    assert result.execution_time >= 0

    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_thread_task():
    engine = ParallelExecutionEngine(max_concurrent_tasks=5)

    def compute(x):
        return x * x

    task_id = await engine.submit_task(func=compute, args=(7,), mode=ExecutionMode.THREAD)
    result = await engine.wait_for_task(task_id, timeout=2.0)

    assert result.status == "completed"
    assert result.result == 49
    assert result.error is None

    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_task_timeout():
    engine = ParallelExecutionEngine(max_concurrent_tasks=5)

    async def slow():
        await asyncio.sleep(0.5)
        return "done"

    task_id = await engine.submit_task(func=slow, mode=ExecutionMode.ASYNC)
    result = await engine.wait_for_task(task_id, timeout=0.05)

    assert result.status == "timeout"
    assert isinstance(result.error, TimeoutError)

    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_task_error():
    engine = ParallelExecutionEngine(max_concurrent_tasks=5)

    async def fail():
        raise ValueError("Something went wrong")

    task_id = await engine.submit_task(func=fail, mode=ExecutionMode.ASYNC)
    result = await engine.wait_for_task(task_id, timeout=2.0)

    assert result.status == "error"
    assert isinstance(result.error, ValueError)
    assert "Something went wrong" in str(result.error)

    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_batch_and_map():
    engine = ParallelExecutionEngine(max_concurrent_tasks=5)

    async def double(n):
        await asyncio.sleep(0.01)
        return n * 2

    # Test map_parallel
    results = await engine.map_parallel(func=double, items=[1, 2, 3, 4], batch_size=2)
    assert len(results) == 4
    assert [r.result for r in results] == [2, 4, 6, 8]
    assert all(r.status == "completed" for r in results)

    # Test submit_batch
    batch_configs = [
        {"func": double, "args": (10,), "mode": ExecutionMode.ASYNC},
        {"func": double, "args": (20,), "mode": ExecutionMode.ASYNC},
    ]
    task_ids = await engine.submit_batch(batch_configs)
    batch_results = await engine.wait_for_all(task_ids, timeout=2.0)
    assert len(batch_results) == 2
    assert [r.result for r in batch_results] == [20, 40]

    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_pipeline_and_status():
    engine = ParallelExecutionEngine(max_concurrent_tasks=5)

    execution_order = []

    async def step(name):
        await asyncio.sleep(0.01)
        execution_order.append(name)
        return name

    pipeline_configs = [
        {"func": step, "args": ("step1",), "mode": ExecutionMode.ASYNC},
        {"func": step, "args": ("step2",), "mode": ExecutionMode.ASYNC},
        {"func": step, "args": ("step3",), "mode": ExecutionMode.ASYNC},
    ]

    task_ids = await engine.pipeline_execution(pipeline_configs)
    results = await engine.wait_for_all(task_ids, timeout=3.0)

    assert len(results) == 3
    assert all(r.status == "completed" for r in results)
    assert execution_order == ["step1", "step2", "step3"]

    status = engine.get_status()
    assert "active_tasks" in status
    assert "completed_tasks" in status
    assert status["completed_tasks"] >= 3

    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_process_mode():
    engine = ParallelExecutionEngine(max_concurrent_tasks=2, process_workers=2)
    task_id = await engine.submit_task(func=_process_worker_cube, args=(4,), mode=ExecutionMode.PROCESS)
    res = await engine.wait_for_task(task_id, timeout=5.0)

    assert res.status == "completed"
    assert res.result == 64
    assert res.error is None
    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_submit_task_async_timeout():
    engine = ParallelExecutionEngine(max_concurrent_tasks=2)

    async def slow():
        await asyncio.sleep(0.5)
        return "slow_done"

    task_id = await engine.submit_task(func=slow, mode=ExecutionMode.ASYNC, timeout=0.05)
    res = await engine.wait_for_task(task_id, timeout=2.0)

    assert res.status == "timeout"
    assert isinstance(res.error, (asyncio.TimeoutError, TimeoutError))
    await engine.shutdown()


@pytest.mark.asyncio
async def test_parallel_engine_multiple_pipeline_runs():
    engine = ParallelExecutionEngine(max_concurrent_tasks=5)

    async def mult(val):
        await asyncio.sleep(0.01)
        return val * 10

    # Run 1
    tasks_1 = [{"func": mult, "args": (2,), "mode": ExecutionMode.ASYNC}]
    ids_1 = await engine.pipeline_execution(tasks_1)
    res_1 = await engine.wait_for_all(ids_1)
    assert res_1[0].result == 20

    # Run 2
    tasks_2 = [{"func": mult, "args": (7,), "mode": ExecutionMode.ASYNC}]
    ids_2 = await engine.pipeline_execution(tasks_2)
    res_2 = await engine.wait_for_all(ids_2)
    assert res_2[0].result == 70

    # IDs must be different to prevent stale cache returns
    assert ids_1 != ids_2

    await engine.shutdown()


def test_parallel_engine_max_threads_alias():
    engine = ParallelExecutionEngine(max_threads=12)
    assert engine.worker_pool.thread_workers == 12
