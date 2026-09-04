import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from core.agent_base import AgentCapability, Task, BaseAgent
from core.communication import Message, MessageType, InMemoryMessageBus
from core.swarm_orchestrator import SwarmOrchestrator
from core.api_integration import WebSocketAPIClient
from core.monitoring import MetricsCollector, AlertManager, AlertRule, Alert


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


def test_message_from_dict_naive_iso_timestamp():
    # Naive timestamp string without offset should be normalized to UTC
    raw = {
        "id": "naive-1",
        "sender_id": "s1",
        "recipient_id": "r1",
        "message_type": "event",
        "payload": {},
        "timestamp": "2026-09-04T12:00:00",
        "correlation_id": None
    }
    restored = Message.from_dict(raw)
    assert restored.timestamp.tzinfo == timezone.utc


async def test_agent_base_emit_event_sync_and_async_handlers():
    class TestAgent(BaseAgent):
        async def initialize(self):
            pass

        async def execute_task(self, task):
            return {}

        async def cleanup(self):
            pass

    agent = TestAgent()
    called = []

    def sync_handler(ag, event, data):
        called.append(("sync", event, data["val"]))

    async def async_handler(ag, event, data):
        called.append(("async", event, data["val"]))

    agent.register_event_handler("custom_event", sync_handler)
    agent.register_event_handler("custom_event", async_handler)

    await agent._emit_event("custom_event", {"val": 42})

    assert ("sync", "custom_event", 42) in called
    assert ("async", "custom_event", 42) in called


async def test_websocket_client_sync_and_async_handlers():
    client = WebSocketAPIClient(url="ws://mock", name="test_ws")
    calls = []

    def sync_handler(data):
        calls.append(("sync", data))

    async def async_handler(data):
        calls.append(("async", data))

    client.add_message_handler(sync_handler)
    client.add_message_handler(async_handler)

    # Mock websocket with one message then None to exit _listen
    mock_ws = AsyncMock()
    mock_ws.recv.side_effect = [
        json.dumps({"type": "hello"}),
        Exception("disconnect")
    ]
    client.websocket = mock_ws

    await client._listen()

    assert len(calls) == 2
    assert ("sync", {"type": "hello"}) in calls
    assert ("async", {"type": "hello"}) in calls


async def test_metrics_and_alerts_timezone():
    collector = MetricsCollector()
    sys_metrics = await collector._collect_system_metrics()
    assert sys_metrics.timestamp.tzinfo == timezone.utc

    alert_mgr = AlertManager()
    triggered = []

    class TriggerRule(AlertRule):
        async def evaluate(self, sm, am):
            return True

    alert_mgr.add_alert_rule(TriggerRule(name="test", severity="low", message="alert msg"))
    alert_mgr.add_alert_handler(lambda a: triggered.append(a))

    await alert_mgr.check_alerts(sys_metrics, {})
    assert len(triggered) == 1
    assert triggered[0].timestamp.tzinfo == timezone.utc

    alert_mgr.resolve_alert(triggered[0].id)
    assert triggered[0].resolved is True
    assert triggered[0].resolved_at.tzinfo == timezone.utc


async def test_swarm_register_agent_limit():
    class DummyAgent(BaseAgent):
        async def initialize(self):
            pass

        async def execute_task(self, task):
            return {}

        async def cleanup(self):
            pass

    swarm = SwarmOrchestrator(max_agents=1)
    a1 = DummyAgent()
    a2 = DummyAgent()
    ok1 = await swarm.register_agent(a1)
    ok2 = await swarm.register_agent(a2)
    assert ok1 is True
    assert ok2 is False
