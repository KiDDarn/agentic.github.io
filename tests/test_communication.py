import asyncio
from datetime import datetime, timezone

import pytest

from core.communication import (
    CommunicationProtocol,
    CoordinationService,
    InMemoryMessageBus,
    Message,
    MessageType,
)


def test_message_serialization_and_deserialization():
    now = datetime.now(timezone.utc)
    msg = Message(
        id="m-1",
        sender_id="sender-1",
        recipient_id="recipient-1",
        message_type=MessageType.TASK_REQUEST,
        payload={"action": "run"},
        timestamp=now,
        correlation_id="corr-1",
    )

    data = msg.to_dict()
    assert data["id"] == "m-1"
    assert data["message_type"] == "task_request"
    assert data["correlation_id"] == "corr-1"
    assert data["timestamp"] == now.isoformat()

    restored = Message.from_dict(data)
    assert restored.id == msg.id
    assert restored.sender_id == msg.sender_id
    assert restored.recipient_id == msg.recipient_id
    assert restored.message_type == MessageType.TASK_REQUEST
    assert restored.payload == msg.payload
    assert restored.timestamp.tzinfo is not None
    assert restored.correlation_id == msg.correlation_id


@pytest.mark.asyncio
async def test_in_memory_message_bus_error_handling_and_unsubscribe():
    bus = InMemoryMessageBus()
    handled = []

    def failing_handler(msg: Message):
        raise RuntimeError("Handler failed on purpose")

    async def successful_handler(msg: Message):
        handled.append(msg.id)

    await bus.subscribe("events", failing_handler)
    await bus.subscribe("events", successful_handler)

    msg = Message(
        id="msg-err-test",
        sender_id="s",
        recipient_id="r",
        message_type=MessageType.EVENT,
        payload={},
        timestamp=datetime.now(timezone.utc),
    )

    # Bus should not crash when one handler raises
    await bus.publish("events", msg)
    assert "msg-err-test" in handled

    # Publishing to nonexistent channel
    await bus.publish("nonexistent", msg)

    # Unsubscribe
    await bus.unsubscribe("events")
    handled.clear()
    await bus.publish("events", msg)
    assert len(handled) == 0


@pytest.mark.asyncio
async def test_communication_protocol_request_response_and_handlers():
    bus = InMemoryMessageBus()

    client_proto = CommunicationProtocol("client", bus)
    server_proto = CommunicationProtocol("server", bus)

    # Yield to let _setup_default_channels finish subscribing
    await asyncio.sleep(0.01)

    # Server handler for TASK_REQUEST
    async def server_task_handler(msg: Message):
        await server_proto.send_response(
            original_message=msg,
            response_payload={"status": "accepted", "task_id": msg.payload.get("task_id")},
        )

    server_proto.register_handler(MessageType.TASK_REQUEST, server_task_handler)

    # Client sends request to server
    response = await client_proto.send_request(
        recipient_id="server",
        message_type=MessageType.TASK_REQUEST,
        payload={"task_id": "t-100"},
        timeout=2.0,
    )

    assert response.message_type == MessageType.TASK_RESPONSE
    assert response.payload["status"] == "accepted"
    assert response.payload["task_id"] == "t-100"
    assert response.correlation_id is not None


@pytest.mark.asyncio
async def test_communication_protocol_timeout():
    bus = InMemoryMessageBus()
    proto = CommunicationProtocol("lonely-agent", bus)
    await asyncio.sleep(0.01)

    with pytest.raises(asyncio.TimeoutError):
        await proto.send_request(
            recipient_id="nobody",
            message_type=MessageType.TASK_REQUEST,
            payload={},
            timeout=0.05,
        )


@pytest.mark.asyncio
async def test_coordination_service_lifecycle():
    bus = InMemoryMessageBus()
    coord = CoordinationService(bus)
    worker_proto = CommunicationProtocol("worker-1", bus)
    await asyncio.sleep(0.01)

    # Send heartbeat from worker
    await worker_proto.send_heartbeat({"state": "running", "queue_size": 2})
    await asyncio.sleep(0.01)

    assert "worker-1" in coord.active_agents
    info = coord.active_agents["worker-1"]
    assert info["status"]["state"] == "running"
    assert info["status"]["queue_size"] == 2

    # Send status update
    await worker_proto.send_message(
        recipient_id="coordinator",
        message_type=MessageType.AGENT_STATUS,
        payload={"load": 0.45},
    )
    await asyncio.sleep(0.01)
    assert coord.active_agents["worker-1"]["load"] == 0.45

    # Setup worker to accept task requests
    async def task_accept_handler(msg: Message):
        await worker_proto.send_response(msg, {"accepted": True, "task_id": msg.payload["task_id"]})

    worker_proto.register_handler(MessageType.TASK_REQUEST, task_accept_handler)

    # Assign task via coordinator
    assigned = await coord.assign_task(task_id="t-200", task_data={"op": "calc"}, target_agent="worker-1")
    assert assigned is True
    assert coord.task_assignments["t-200"] == "worker-1"

    # Assign task to nonexistent agent
    assert await coord.assign_task(task_id="t-201", task_data={}, target_agent="nonexistent") is False

    # Broadcast system command
    commands_received = []

    def cmd_handler(msg: Message):
        commands_received.append(msg.payload)

    worker_proto.register_handler(MessageType.SYSTEM_COMMAND, cmd_handler)
    await coord.broadcast_system_command("PAUSE", {"reason": "maintenance"})
    await asyncio.sleep(0.01)

    assert len(commands_received) == 1
    assert commands_received[0]["command"] == "PAUSE"

    status = coord.get_swarm_status()
    assert status["active_agents"] == 1
    assert "worker-1" in status["agents"]
    assert "t-200" in status["task_assignments"]


@pytest.mark.asyncio
async def test_selective_unsubscribe_and_protocol_cleanup():
    bus = InMemoryMessageBus()
    handled_1 = []
    handled_2 = []

    def h1(msg: Message):
        handled_1.append(msg.id)

    def h2(msg: Message):
        handled_2.append(msg.id)

    await bus.subscribe("shared_chan", h1)
    await bus.subscribe("shared_chan", h2)
    # Duplicate subscription should be ignored
    await bus.subscribe("shared_chan", h1)

    msg = Message("m-sub", "s", "r", MessageType.EVENT, {}, datetime.now(timezone.utc))
    await bus.publish("shared_chan", msg)

    assert len(handled_1) == 1
    assert len(handled_2) == 1

    # Unsubscribe only h1
    await bus.unsubscribe("shared_chan", h1)

    msg2 = Message("m-sub-2", "s", "r", MessageType.EVENT, {}, datetime.now(timezone.utc))
    await bus.publish("shared_chan", msg2)

    assert len(handled_1) == 1
    assert len(handled_2) == 2

    # Protocol initialization idempotence and cleanup
    proto = CommunicationProtocol("test-clean", bus)
    await proto.initialize()
    await proto.initialize()  # idempotent
    assert "broadcast" in bus.subscriptions

    await proto.cleanup()
    # Handler should be unsubscribed from bus
    assert proto._handle_message not in bus.subscriptions.get("broadcast", [])
