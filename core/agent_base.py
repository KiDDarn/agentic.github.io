import asyncio
import inspect
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List


class AgentState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    FAILED = "failed"
    TERMINATED = "terminated"


@dataclass
class AgentCapability:
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]


@dataclass
class Task:
    id: str
    agent_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: int = 0
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


@dataclass
class AgentMetrics:
    tasks_completed: int = 0
    tasks_failed: int = 0
    avg_execution_time: float = 0.0
    uptime: float = 0.0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0


class BaseAgent(ABC):
    def __init__(self, agent_id: str = None, name: str = None):
        self.id = agent_id or str(uuid.uuid4())
        self.name = name or f"Agent-{self.id[:8]}"
        self.state = AgentState.IDLE
        self.capabilities: List[AgentCapability] = []
        self.metrics = AgentMetrics()
        self.logger = logging.getLogger(f"agent.{self.name}")
        self.task_queue = asyncio.Queue()
        self.api_clients: Dict[str, Any] = {}
        self.event_handlers: Dict[str, List[Callable]] = {}

    @abstractmethod
    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def execute_task(self, task: Task) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        pass

    async def start(self):
        self.state = AgentState.RUNNING
        try:
            await self.initialize()
        except Exception as e:
            self.state = AgentState.FAILED
            self.logger.error(f"Agent {self.name} failed to initialize: {e}")
            await self._emit_event("agent_failed", {"error": str(e)})
            return

        self.logger.info(f"Agent {self.name} started")

        while self.state != AgentState.TERMINATED:
            if self.state == AgentState.PAUSED:
                await asyncio.sleep(0.05)
                continue
            try:
                task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                result = await self.execute_task(task)
                self.metrics.tasks_completed += 1
                await self._emit_event("task_completed", {"task_id": task.id, "result": result})
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.metrics.tasks_failed += 1
                self.logger.error(f"Task execution failed: {e}")
                await self._emit_event("task_failed", {"task_id": getattr(task, "id", None), "error": str(e)})

    async def stop(self):
        self.state = AgentState.TERMINATED
        await self.cleanup()
        self.logger.info(f"Agent {self.name} stopped")

    async def pause(self):
        self.state = AgentState.PAUSED

    async def resume(self):
        self.state = AgentState.RUNNING

    async def add_task(self, task: Task):
        await self.task_queue.put(task)

    def register_capability(self, capability: AgentCapability):
        self.capabilities.append(capability)

    def add_api_client(self, name: str, client: Any):
        self.api_clients[name] = client

    def register_event_handler(self, event: str, handler: Callable):
        if event not in self.event_handlers:
            self.event_handlers[event] = []
        self.event_handlers[event].append(handler)

    async def _emit_event(self, event: str, data: Dict[str, Any]):
        if event in self.event_handlers:
            for handler in self.event_handlers[event]:
                try:
                    if inspect.iscoroutinefunction(handler):
                        await handler(self, event, data)
                    else:
                        handler(self, event, data)
                except Exception as e:
                    self.logger.error(f"Event handler failed: {e}")
