import asyncio

import pytest

from core.agent_base import AgentCapability, AgentState, BaseAgent, Task
from core.swarm_orchestrator import (
    LoadBalancer,
    SwarmMetrics,
    SwarmOrchestrator,
    TaskRouter,
)


class MockWorkerAgent(BaseAgent):
    def __init__(self, name="Worker", capabilities=None):
        super().__init__(name=name)
        self.init_done = False
        self.cleaned_up = False
        if capabilities:
            for c in capabilities:
                self.register_capability(c)

    async def initialize(self):
        self.init_done = True

    async def execute_task(self, task: Task):
        await asyncio.sleep(0.01)
        return {"result": f"processed_{task.id}"}

    async def cleanup(self):
        self.cleaned_up = True


def test_swarm_metrics_defaults():
    m = SwarmMetrics()
    assert m.total_agents == 0
    assert m.active_agents == 0
    assert m.total_tasks_processed == 0


def test_task_router_and_load_balancer():
    router = TaskRouter()
    balancer = LoadBalancer()

    cap_data = AgentCapability("data_processing", "process", {}, {})
    cap_text = AgentCapability("text_processing", "text", {}, {})

    a1 = MockWorkerAgent(name="A1", capabilities=[cap_data])
    a2 = MockWorkerAgent(name="A2", capabilities=[cap_data])
    a3 = MockWorkerAgent(name="A3", capabilities=[cap_text])

    task_data = Task(id="t1", agent_id="", task_type="data_processing", payload={})

    # None are running yet, so router should find 0
    assert router.find_suitable_agents(task_data, [a1, a2, a3]) == []

    # Mark a1 and a2 as RUNNING
    a1.state = AgentState.RUNNING
    a2.state = AgentState.RUNNING
    a3.state = AgentState.RUNNING

    suitable = router.find_suitable_agents(task_data, [a1, a2, a3])
    assert len(suitable) == 2
    assert a1 in suitable and a2 in suitable
    assert a3 not in suitable

    # Put a task in a1's queue
    a1.task_queue.put_nowait(task_data)
    assert a1.task_queue.qsize() == 1
    assert a2.task_queue.qsize() == 0

    # LoadBalancer should pick a2 because its queue size is 0
    selected = balancer.select_agent(suitable)
    assert selected == a2

    # Empty agent list should return None
    assert balancer.select_agent([]) is None


@pytest.mark.asyncio
async def test_swarm_agent_registration_and_duplicates():
    swarm = SwarmOrchestrator(max_agents=2)
    a1 = MockWorkerAgent(name="A1")

    assert await swarm.register_agent(a1) is True
    # Duplicate registration should return False
    assert await swarm.register_agent(a1) is False
    assert swarm.metrics.total_agents == 1

    a2 = MockWorkerAgent(name="A2")
    assert await swarm.register_agent(a2) is True

    # Exceeding limit
    a3 = MockWorkerAgent(name="A3")
    assert await swarm.register_agent(a3) is False
    assert swarm.metrics.total_agents == 2


@pytest.mark.asyncio
async def test_swarm_agent_lifecycle_and_status():
    swarm = SwarmOrchestrator(max_agents=5)
    a1 = MockWorkerAgent(name="A1")
    await swarm.register_agent(a1)

    # Start unknown agent
    assert await swarm.start_agent("unknown-id") is False

    # Start registered agent
    assert await swarm.start_agent(a1.id) is True
    await asyncio.sleep(0.01)
    assert swarm.metrics.active_agents == 1
    # Starting already running agent
    assert await swarm.start_agent(a1.id) is False

    # Check agent status
    status = swarm.get_agent_status(a1.id)
    assert status is not None
    assert status["id"] == a1.id
    assert status["name"] == "A1"
    assert status["state"] == AgentState.RUNNING.value

    # Unknown agent status
    assert swarm.get_agent_status("unknown") is None

    # Swarm status
    swarm_status = swarm.get_swarm_status()
    assert swarm_status["metrics"].active_agents == 1
    assert a1.id in swarm_status["agents"]

    # Stop unknown agent
    assert await swarm.stop_agent("unknown-id") is False

    # Stop registered agent
    assert await swarm.stop_agent(a1.id) is True
    assert swarm.metrics.active_agents == 0
    assert a1.cleaned_up is True

    # Remove agent
    assert await swarm.remove_agent(a1.id) is True
    assert swarm.metrics.total_agents == 0
    assert await swarm.remove_agent(a1.id) is False


@pytest.mark.asyncio
async def test_swarm_task_submission_and_broadcast():
    swarm = SwarmOrchestrator(max_agents=5)
    cap = AgentCapability("compute", "compute something", {}, {})

    a1 = MockWorkerAgent(name="A1", capabilities=[cap])
    a2 = MockWorkerAgent(name="A2", capabilities=[cap])
    await swarm.register_agent(a1)
    await swarm.register_agent(a2)

    await swarm.start_agent(a1.id)
    await swarm.start_agent(a2.id)
    await asyncio.sleep(0.01)

    # Submit task with matching capability
    task = Task(id="t-calc", agent_id="", task_type="compute", payload={"x": 5})
    ok = await swarm.submit_task(task)
    assert ok is True
    assert swarm.metrics.total_tasks_processed == 1

    # Submit task with no matching capability
    unmatched_task = Task(id="t-unmatched", agent_id="", task_type="nonexistent", payload={})
    assert await swarm.submit_task(unmatched_task) is False

    # Broadcast task to all running agents
    b_task = Task(id="t-bcast", agent_id="", task_type="compute", payload={})
    count = await swarm.broadcast_task(b_task)
    assert count == 2
    assert swarm.metrics.total_tasks_processed == 3

    # Stop swarm
    await swarm.stop_swarm()
    assert swarm.is_running is False


@pytest.mark.asyncio
async def test_swarm_start_and_stop_lifecycle():
    swarm = SwarmOrchestrator(max_agents=3)
    cap = AgentCapability("compute", "compute something", {}, {})
    a1 = MockWorkerAgent(name="A1", capabilities=[cap])
    await swarm.register_agent(a1)

    # start_swarm should return promptly without blocking
    await asyncio.wait_for(swarm.start_swarm(), timeout=1.0)
    assert swarm.is_running is True
    assert swarm.monitoring_task is not None
    assert not swarm.monitoring_task.done()

    # Agent should be running now
    assert a1.state == AgentState.RUNNING

    # Stop swarm
    await swarm.stop_swarm()
    assert swarm.is_running is False
    assert swarm.monitoring_task.done()
