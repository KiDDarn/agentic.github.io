import asyncio

from datetime import timezone
from core.agent_base import AgentCapability, Task, BaseAgent
from core.communication import Message, MessageType, InMemoryMessageBus
from core.swarm_orchestrator import SwarmOrchestrator


def test_agent_capability_init():
    cap = AgentCapability(name="x", description="d", input_schema={}, output_schema={})
    assert cap.name == "x"


def test_task_post_init():
    t = Task(id="t1", agent_id="", task_type="general", payload={})
    assert t.created_at is not None
    assert t.created_at.tzinfo == timezone.utc


async def test_in_memory_message_bus_handlers():
    bus = InMemoryMessageBus()
    received = []

    def sync_handler(msg: Message):
        received.append(("sync", msg.id))

    async def async_handler(msg: Message):
        received.append(("async", msg.id))

    await bus.subscribe("test.channel", sync_handler)
    await bus.subscribe("test.channel", async_handler)

    msg = Message(
        id="msg-1",
        sender_id="s1",
        recipient_id="r1",
        message_type=MessageType.EVENT,
        payload={"data": 123},
        timestamp=Task(id="dummy", agent_id="", task_type="", payload={}).created_at,
    )
    await bus.publish("test.channel", msg)

    assert len(received) == 2
    assert ("sync", "msg-1") in received
    assert ("async", "msg-1") in received

    # test serialization roundtrip
    msg_dict = msg.to_dict()
    restored = Message.from_dict(msg_dict)
    assert restored.id == msg.id
    assert restored.timestamp.tzinfo is not None


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
