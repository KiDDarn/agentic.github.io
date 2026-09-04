import asyncio
import logging
import multiprocessing
import time
import uuid
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from queue import PriorityQueue
from typing import Any, Callable, Dict, List, Optional


class ExecutionMode(Enum):
    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"
    DISTRIBUTED = "distributed"


@dataclass
class ExecutionTask:
    id: str
    func: Callable
    args: tuple = ()
    kwargs: dict = None
    priority: int = 0
    mode: ExecutionMode = ExecutionMode.ASYNC
    timeout: float = None
    dependencies: List[str] = None

    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}
        if self.dependencies is None:
            self.dependencies = []


@dataclass
class ExecutionResult:
    task_id: str
    result: Any = None
    error: Optional[Exception] = None
    execution_time: float = 0.0
    status: str = "pending"


class WorkerPool:
    def __init__(self, thread_workers: int = None, process_workers: int = None):
        self.thread_workers = thread_workers or min(32, (multiprocessing.cpu_count() or 1) + 4)
        self.process_workers = process_workers or multiprocessing.cpu_count()

        self.thread_executor = ThreadPoolExecutor(max_workers=self.thread_workers)
        self.process_executor = ProcessPoolExecutor(max_workers=self.process_workers)

        self.logger = logging.getLogger("parallel.worker_pool")

    async def submit_task(self, task: ExecutionTask) -> Future:
        if task.mode == ExecutionMode.THREAD:
            return self.thread_executor.submit(task.func, *task.args, **task.kwargs)
        elif task.mode == ExecutionMode.PROCESS:
            return self.process_executor.submit(task.func, *task.args, **task.kwargs)
        elif task.mode == ExecutionMode.ASYNC:
            return asyncio.create_task(task.func(*task.args, **task.kwargs))
        else:
            raise ValueError(f"Unsupported execution mode: {task.mode}")

    async def shutdown(self):
        await asyncio.to_thread(self.thread_executor.shutdown, wait=False)
        await asyncio.to_thread(self.process_executor.shutdown, wait=False)


class TaskScheduler:
    def __init__(self):
        self.task_queue = PriorityQueue()
        self.completed_tasks: Dict[str, ExecutionResult] = {}
        self.pending_tasks: Dict[str, ExecutionTask] = {}
        self.task_dependencies: Dict[str, List[str]] = {}
        self.logger = logging.getLogger("parallel.scheduler")

    def add_task(self, task: ExecutionTask):
        self.pending_tasks[task.id] = task
        self.task_dependencies[task.id] = task.dependencies.copy()

        if not task.dependencies:
            self.task_queue.put((task.priority, task.id))

        self.logger.info(f"Added task {task.id} with priority {task.priority}")

    def get_ready_task(self) -> Optional[ExecutionTask]:
        if self.task_queue.empty():
            return None

        _, task_id = self.task_queue.get()
        if task_id in self.pending_tasks:
            task = self.pending_tasks.pop(task_id)
            return task
        return None

    def mark_task_completed(self, task_id: str, result: ExecutionResult):
        self.completed_tasks[task_id] = result

        for pending_id, dependencies in self.task_dependencies.items():
            if task_id in dependencies:
                dependencies.remove(task_id)
                if not dependencies and pending_id in self.pending_tasks:
                    task = self.pending_tasks[pending_id]
                    self.task_queue.put((task.priority, pending_id))

        self.logger.info(f"Completed task {task_id}")

    def get_completed_result(self, task_id: str) -> Optional[ExecutionResult]:
        return self.completed_tasks.get(task_id)


class ParallelExecutionEngine:
    def __init__(
        self,
        max_concurrent_tasks: int = 100,
        thread_workers: Optional[int] = None,
        process_workers: Optional[int] = None,
        max_threads: Optional[int] = None,
    ):
        self.max_concurrent_tasks = max_concurrent_tasks
        workers = thread_workers if thread_workers is not None else max_threads
        self.worker_pool = WorkerPool(workers, process_workers)
        self.scheduler = TaskScheduler()
        self.active_futures: Dict[str, Future] = {}
        self.task_results: Dict[str, ExecutionResult] = {}
        self.is_running = False
        self.logger = logging.getLogger("parallel.engine")

    async def submit_task(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        priority: int = 0,
        mode: ExecutionMode = ExecutionMode.ASYNC,
        timeout: float = None,
        dependencies: List[str] = None,
        task_id: str = None,
    ) -> str:
        if task_id is None:
            task_id = str(uuid.uuid4())

        if task_id in self.task_results:
            del self.task_results[task_id]

        task = ExecutionTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            priority=priority,
            mode=mode,
            timeout=timeout,
            dependencies=dependencies or [],
        )

        self.scheduler.add_task(task)

        if not self.is_running:
            asyncio.create_task(self._process_tasks())
            self.is_running = True

        return task_id

    async def submit_batch(self, tasks: List[Dict[str, Any]]) -> List[str]:
        task_ids = []

        for task_config in tasks:
            task_id = await self.submit_task(**task_config)
            task_ids.append(task_id)

        return task_ids

    async def wait_for_task(self, task_id: str, timeout: float = None) -> ExecutionResult:
        start_time = time.time()

        while task_id not in self.task_results:
            if timeout and (time.time() - start_time) > timeout:
                return ExecutionResult(task_id=task_id, error=TimeoutError("Task execution timeout"), status="timeout")

            await asyncio.sleep(0.01)

        return self.task_results[task_id]

    async def wait_for_all(self, task_ids: List[str], timeout: float = None) -> List[ExecutionResult]:
        results = []

        for task_id in task_ids:
            result = await self.wait_for_task(task_id, timeout)
            results.append(result)

        return results

    async def map_parallel(
        self, func: Callable, items: List[Any], mode: ExecutionMode = ExecutionMode.ASYNC, batch_size: int = None
    ) -> List[ExecutionResult]:
        task_ids = []

        batch_size = batch_size or len(items)

        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]

            for item in batch:
                task_id = await self.submit_task(func=func, args=(item,), mode=mode)
                task_ids.append(task_id)

        return await self.wait_for_all(task_ids)

    async def pipeline_execution(self, pipeline_tasks: List[Dict[str, Any]]) -> List[str]:
        task_ids = []
        previous_task_id = None
        pipeline_prefix = f"pipeline_{uuid.uuid4().hex[:8]}"

        for i, task_cfg in enumerate(pipeline_tasks):
            task_config = dict(task_cfg)
            dependencies = list(task_config.get("dependencies") or [])
            if previous_task_id:
                dependencies.append(previous_task_id)

            task_config["dependencies"] = dependencies
            task_config["task_id"] = task_config.get("task_id") or f"{pipeline_prefix}_step_{i}"

            task_id = await self.submit_task(**task_config)
            task_ids.append(task_id)
            previous_task_id = task_id

        return task_ids

    async def _process_tasks(self):
        while True:
            if len(self.active_futures) >= self.max_concurrent_tasks:
                await asyncio.sleep(0.01)
                continue

            task = self.scheduler.get_ready_task()
            if not task:
                if not self.active_futures and self.scheduler.task_queue.empty() and not self.scheduler.pending_tasks:
                    break
                await asyncio.sleep(0.01)
                continue

            try:
                future = await self.worker_pool.submit_task(task)
                self.active_futures[task.id] = future

                asyncio.create_task(self._handle_task_completion(task, future))

            except Exception as e:
                result = ExecutionResult(task_id=task.id, error=e, status="error")
                self.task_results[task.id] = result
                self.scheduler.mark_task_completed(task.id, result)

        self.is_running = False

    async def _handle_task_completion(self, task: ExecutionTask, future: Future):
        start_time = time.time()
        result = None

        try:
            if task.mode == ExecutionMode.ASYNC:
                if task.timeout:
                    result_value = await asyncio.wait_for(future, timeout=task.timeout)
                else:
                    result_value = await future
            else:
                if task.timeout:
                    result_value = await asyncio.wait_for(asyncio.wrap_future(future), timeout=task.timeout)
                else:
                    result_value = await asyncio.wrap_future(future)

            result = ExecutionResult(
                task_id=task.id, result=result_value, execution_time=time.time() - start_time, status="completed"
            )

        except (asyncio.TimeoutError, TimeoutError) as e:
            result = ExecutionResult(
                task_id=task.id, error=e, execution_time=time.time() - start_time, status="timeout"
            )
        except asyncio.CancelledError:
            result = ExecutionResult(
                task_id=task.id,
                error=asyncio.CancelledError("Task was cancelled"),
                execution_time=time.time() - start_time,
                status="cancelled",
            )
            raise
        except Exception as e:
            result = ExecutionResult(task_id=task.id, error=e, execution_time=time.time() - start_time, status="error")

        finally:
            if result is not None:
                self.task_results[task.id] = result
                self.scheduler.mark_task_completed(task.id, result)
            if task.id in self.active_futures:
                del self.active_futures[task.id]

    async def shutdown(self):
        self.is_running = False
        await self.worker_pool.shutdown()
        self.logger.info("Parallel execution engine shutdown")

    def get_status(self) -> Dict[str, Any]:
        return {
            "active_tasks": len(self.active_futures),
            "completed_tasks": len(self.task_results),
            "pending_tasks": len(self.scheduler.pending_tasks),
            "is_running": self.is_running,
        }
