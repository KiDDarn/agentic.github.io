import asyncio

from core.agent_base import AgentCapability, Task, BaseAgent
from core.swarm_orchestrator import SwarmOrchestrator


def test_agent_capability_init():
    cap = AgentCapability(name="x", description="d", input_schema={}, output_schema={})
    assert cap.name == "x"


def test_task_post_init():
    t = Task(id="t1", agent_id="", task_type="general", payload={})
    assert t.created_at is not None


def test_swarm_register_agent_limit():
    class DummyAgent(BaseAgent):
        async def initialize(self):
            pass

        async def execute_task(self, task):
            return {}

        async def cleanup(self):
            pass

    async def run_test():
        swarm = SwarmOrchestrator(max_agents=1)
        a1 = DummyAgent()
        a2 = DummyAgent()
        ok1 = await swarm.register_agent(a1)
        ok2 = await swarm.register_agent(a2)
        assert ok1 is True
        assert ok2 is False

    asyncio.run(run_test())
