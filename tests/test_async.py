import asyncio
import pytest

from core.agent_base import BaseAgent, Task
from core.agent_base import AgentState


class DummyAsyncAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="DummyAsync")
        self.init_called = False
        self.cleaned = False

    async def initialize(self):
        self.init_called = True

    async def execute_task(self, task: Task):
        # simulate a tiny async workload
        await asyncio.sleep(0.01)
        return {"ok": True}

    async def cleanup(self):
        self.cleaned = True


@pytest.mark.asyncio
async def test_agent_start_and_stop():
    agent = DummyAsyncAgent()

    # start agent in background
    task = asyncio.create_task(agent.start())

    # give it a moment to initialize
    await asyncio.sleep(0.05)

    assert agent.state == AgentState.RUNNING
    assert agent.init_called

    # stop and wait for background task to finish
    await agent.stop()

    # allow background loop to exit
    await asyncio.wait_for(asyncio.shield(task), timeout=1.0)

    assert agent.state == AgentState.TERMINATED
    assert agent.cleaned
