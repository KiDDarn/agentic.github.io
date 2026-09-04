import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .agent_base import AgentState, BaseAgent, Task


@dataclass
class SwarmMetrics:
    total_agents: int = 0
    active_agents: int = 0
    total_tasks_processed: int = 0
    avg_task_completion_time: float = 0.0
    system_uptime: float = 0.0


class SwarmOrchestrator:
    def __init__(self, max_agents: int = 100, max_threads: int = 50):
        self.agents: Dict[str, BaseAgent] = {}
        self.agent_tasks: Dict[str, asyncio.Task] = {}
        self.max_agents = max_agents
        self.max_threads = max_threads
        self.executor = ThreadPoolExecutor(max_workers=max_threads)
        self.logger = logging.getLogger("swarm.orchestrator")
        self.metrics = SwarmMetrics()
        self.task_router = TaskRouter()
        self.load_balancer = LoadBalancer()
        self.is_running = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self._lock = threading.RLock()

    async def register_agent(self, agent: BaseAgent) -> bool:
        with self._lock:
            if len(self.agents) >= self.max_agents:
                self.logger.warning("Maximum agent limit reached")
                return False

            if agent.id in self.agents:
                self.logger.warning(f"Agent {agent.id} already registered")
                return False

            self.agents[agent.id] = agent
            self.metrics.total_agents += 1
            self.logger.info(f"Registered agent {agent.name} ({agent.id})")
            return True

    async def start_agent(self, agent_id: str) -> bool:
        if agent_id not in self.agents:
            self.logger.error(f"Agent {agent_id} not found")
            return False

        agent = self.agents[agent_id]
        if agent_id in self.agent_tasks:
            self.logger.warning(f"Agent {agent_id} already running")
            return False

        task = asyncio.create_task(agent.start())
        self.agent_tasks[agent_id] = task
        self.metrics.active_agents += 1

        self.logger.info(f"Started agent {agent.name}")
        return True

    async def stop_agent(self, agent_id: str) -> bool:
        if agent_id not in self.agents:
            return False

        agent = self.agents[agent_id]
        await agent.stop()

        if agent_id in self.agent_tasks:
            self.agent_tasks[agent_id].cancel()
            del self.agent_tasks[agent_id]
            self.metrics.active_agents -= 1

        return True

    async def remove_agent(self, agent_id: str) -> bool:
        await self.stop_agent(agent_id)

        with self._lock:
            if agent_id in self.agents:
                del self.agents[agent_id]
                self.metrics.total_agents -= 1
                return True
        return False

    async def start_swarm(self):
        self.is_running = True
        self.logger.info("Starting swarm orchestrator")

        for agent_id in self.agents:
            await self.start_agent(agent_id)

        # Yield to the event loop so started agent tasks initialize
        await asyncio.sleep(0)

        self.monitoring_task = asyncio.create_task(self._monitor_agents())

    async def stop_swarm(self):
        self.is_running = False
        self.logger.info("Stopping swarm orchestrator")

        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()

        stop_tasks = []
        for agent_id in list(self.agents.keys()):
            stop_tasks.append(self.stop_agent(agent_id))

        await asyncio.gather(*stop_tasks, return_exceptions=True)
        await asyncio.to_thread(self.executor.shutdown, wait=False)

    async def submit_task(self, task: Task) -> bool:
        suitable_agents = self.task_router.find_suitable_agents(task, list(self.agents.values()))

        if not suitable_agents:
            self.logger.warning(f"No suitable agents for task {task.id}")
            return False

        selected_agent = self.load_balancer.select_agent(suitable_agents)
        await selected_agent.add_task(task)
        self.metrics.total_tasks_processed += 1

        self.logger.info(f"Task {task.id} assigned to agent {selected_agent.name}")
        return True

    async def broadcast_task(self, task: Task) -> int:
        assigned_count = 0
        for agent in self.agents.values():
            if agent.state == AgentState.RUNNING:
                await agent.add_task(task)
                assigned_count += 1

        self.metrics.total_tasks_processed += assigned_count
        return assigned_count

    def get_agent_status(self, agent_id: str) -> Optional[Dict[str, Any]]:
        if agent_id not in self.agents:
            return None

        agent = self.agents[agent_id]
        return {
            "id": agent.id,
            "name": agent.name,
            "state": agent.state.value,
            "capabilities": [cap.name for cap in agent.capabilities],
            "metrics": agent.metrics,
            "queue_size": agent.task_queue.qsize(),
        }

    def get_swarm_status(self) -> Dict[str, Any]:
        return {
            "metrics": self.metrics,
            "agents": {agent_id: self.get_agent_status(agent_id) for agent_id in self.agents},
        }

    async def _monitor_agents(self):
        while self.is_running:
            try:
                failed_agents = []
                for agent_id, task in self.agent_tasks.items():
                    if task.done():
                        failed_agents.append(agent_id)

                for agent_id in failed_agents:
                    self.logger.error(f"Agent {agent_id} task completed unexpectedly")
                    if agent_id in self.agent_tasks:
                        del self.agent_tasks[agent_id]
                        self.metrics.active_agents -= 1

                await asyncio.sleep(5.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Monitor error: {e}")


class TaskRouter:
    def find_suitable_agents(self, task: Task, agents: List[BaseAgent]) -> List[BaseAgent]:
        suitable_agents = []

        for agent in agents:
            if agent.state != AgentState.RUNNING:
                continue

            if self._agent_can_handle_task(agent, task):
                suitable_agents.append(agent)

        return suitable_agents

    def _agent_can_handle_task(self, agent: BaseAgent, task: Task) -> bool:
        for capability in agent.capabilities:
            if capability.name == task.task_type:
                return True
        return False


class LoadBalancer:
    def select_agent(self, agents: List[BaseAgent]) -> BaseAgent:
        if not agents:
            return None

        return min(agents, key=lambda a: a.task_queue.qsize())
