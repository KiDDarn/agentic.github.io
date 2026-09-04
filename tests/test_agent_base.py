import asyncio
from datetime import timezone

import pytest

from core.agent_base import (
    AgentCapability,
    AgentMetrics,
    AgentState,
    BaseAgent,
    Task,
)


class SampleAgent(BaseAgent):
    def __init__(self, name="Sample"):
        super().__init__(name=name)
        self.initialized = False
        self.cleaned_up = False

    async def initialize(self):
        self.initialized = True

    async def execute_task(self, task: Task):
        if task.payload.get("should_fail"):
            raise RuntimeError("Task deliberate failure")
        await asyncio.sleep(0.01)
        return {"done": task.payload.get("val", 0) * 2}

    async def cleanup(self):
        self.cleaned_up = True


def test_agent_dataclasses():
    cap = AgentCapability(
        name="cap1",
        description="desc",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
    )
    assert cap.name == "cap1"

    task = Task(id="t-1", agent_id="ag-1", task_type="general", payload={"x": 1})
    assert task.created_at.tzinfo == timezone.utc
    assert task.priority == 0

    metrics = AgentMetrics()
    assert metrics.tasks_completed == 0
    assert metrics.tasks_failed == 0


@pytest.mark.asyncio
async def test_agent_capabilities_and_clients():
    agent = SampleAgent()
    cap = AgentCapability("summarize", "summarize text", {}, {})
    agent.register_capability(cap)
    assert len(agent.capabilities) == 1
    assert agent.capabilities[0].name == "summarize"

    agent.add_api_client("openai", {"key": "secret"})
    assert "openai" in agent.api_clients

    # Pause and resume
    await agent.pause()
    assert agent.state == AgentState.PAUSED
    await agent.resume()
    assert agent.state == AgentState.RUNNING


@pytest.mark.asyncio
async def test_agent_task_execution_flow_and_metrics():
    agent = SampleAgent()
    events = []

    def sync_handler(ag, event, data):
        events.append((event, data))

    async def async_handler(ag, event, data):
        events.append((f"async_{event}", data))

    agent.register_event_handler("task_completed", sync_handler)
    agent.register_event_handler("task_failed", async_handler)

    # Start agent in background
    run_task = asyncio.create_task(agent.start())
    await asyncio.sleep(0.01)
    assert agent.state == AgentState.RUNNING
    assert agent.initialized is True

    # 1. Add successful task
    t_success = Task(id="t-succ", agent_id=agent.id, task_type="general", payload={"val": 21})
    await agent.add_task(t_success)
    await asyncio.sleep(0.03)

    assert agent.metrics.tasks_completed == 1
    assert any(e[0] == "task_completed" and e[1]["task_id"] == "t-succ" for e in events)

    # 2. Add failing task
    t_fail = Task(id="t-err", agent_id=agent.id, task_type="general", payload={"should_fail": True})
    await agent.add_task(t_fail)
    await asyncio.sleep(0.03)

    assert agent.metrics.tasks_failed == 1
    assert any(e[0] == "async_task_failed" and e[1]["task_id"] == "t-err" for e in events)

    # 3. Stop agent
    await agent.stop()
    await asyncio.wait_for(run_task, timeout=1.5)

    assert agent.state == AgentState.TERMINATED
    assert agent.cleaned_up is True


@pytest.mark.asyncio
async def test_agent_pause_resume_running_lifecycle():
    agent = SampleAgent()
    run_task = asyncio.create_task(agent.start())
    await asyncio.sleep(0.01)
    assert agent.state == AgentState.RUNNING

    # Pause the agent
    await agent.pause()
    assert agent.state == AgentState.PAUSED

    # Wait longer than the 1.0s timeout to prove the task loop does NOT die while paused
    await asyncio.sleep(1.05)
    assert not run_task.done()

    # Resume the agent
    await agent.resume()
    assert agent.state == AgentState.RUNNING
    assert not run_task.done()

    # Add task after resume and verify it completes
    t = Task(id="t-resumed", agent_id=agent.id, task_type="general", payload={"val": 15})
    await agent.add_task(t)
    await asyncio.sleep(0.1)
    assert agent.metrics.tasks_completed == 1

    # Stop cleanly
    await agent.stop()
    await asyncio.wait_for(run_task, timeout=1.0)
    assert agent.state == AgentState.TERMINATED


@pytest.mark.asyncio
async def test_agent_initialization_failure():
    class FailingInitAgent(SampleAgent):
        async def initialize(self):
            raise ValueError("Init failed")

    agent = FailingInitAgent(name="FailingInit")
    events = []

    def failure_handler(ag, event, data):
        events.append((event, data))

    agent.register_event_handler("agent_failed", failure_handler)

    run_task = asyncio.create_task(agent.start())
    await asyncio.sleep(0.01)

    assert agent.state == AgentState.FAILED
    assert len(events) == 1
    assert events[0][0] == "agent_failed"
    assert "Init failed" in events[0][1]["error"]
    assert run_task.done()
