import asyncio
import inspect
import json
import logging
from typing import Dict, Any, List, Callable, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime, timezone
import uuid
import redis.asyncio as redis
from abc import ABC, abstractmethod


class MessageType(Enum):
    TASK_REQUEST = "task_request"
    TASK_RESPONSE = "task_response"
    AGENT_STATUS = "agent_status"
    SYSTEM_COMMAND = "system_command"
    COORDINATION = "coordination"
    HEARTBEAT = "heartbeat"
    EVENT = "event"


@dataclass
class Message:
    id: str
    sender_id: str
    recipient_id: str
    message_type: MessageType
    payload: Dict[str, Any]
    timestamp: datetime
    correlation_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['message_type'] = self.message_type.value
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        data['message_type'] = MessageType(data['message_type'])
        dt = datetime.fromisoformat(data['timestamp'])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        data['timestamp'] = dt
        return cls(**data)


class MessageBus(ABC):
    @abstractmethod
    async def publish(self, channel: str, message: Message) -> None:
        pass
    
    @abstractmethod
    async def subscribe(self, channel: str, handler: Callable[[Message], None]) -> None:
        pass
    
    @abstractmethod
    async def unsubscribe(self, channel: str) -> None:
        pass


class RedisMessageBus(MessageBus):
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client = None
        self.pubsub = None
        self.subscriptions: Dict[str, List[Callable]] = {}
        self.logger = logging.getLogger("communication.redis")
        
    async def initialize(self):
        self.redis_client = redis.from_url(self.redis_url)
        self.pubsub = self.redis_client.pubsub()
        
    async def cleanup(self):
        if self.pubsub:
            await self.pubsub.unsubscribe()
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.close()
    
    async def publish(self, channel: str, message: Message) -> None:
        if not self.redis_client:
            raise RuntimeError("Redis client not initialized")
        
        message_data = json.dumps(message.to_dict())
        await self.redis_client.publish(channel, message_data)
        self.logger.debug(f"Published message to {channel}: {message.id}")
    
    async def subscribe(self, channel: str, handler: Callable[[Message], None]) -> None:
        if channel not in self.subscriptions:
            self.subscriptions[channel] = []
            await self.pubsub.subscribe(channel)
            asyncio.create_task(self._listen_to_channel(channel))
        
        self.subscriptions[channel].append(handler)
        self.logger.info(f"Subscribed to channel: {channel}")
    
    async def unsubscribe(self, channel: str) -> None:
        if channel in self.subscriptions:
            await self.pubsub.unsubscribe(channel)
            del self.subscriptions[channel]
            self.logger.info(f"Unsubscribed from channel: {channel}")
    
    async def _listen_to_channel(self, channel: str):
        async for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    msg = Message.from_dict(data)
                    
                    if channel in self.subscriptions:
                        for handler in self.subscriptions[channel]:
                            try:
                                if inspect.iscoroutinefunction(handler):
                                    await handler(msg)
                                else:
                                    handler(msg)
                            except Exception as e:
                                self.logger.error(f"Handler error: {e}")
                
                except Exception as e:
                    self.logger.error(f"Message processing error: {e}")


class InMemoryMessageBus(MessageBus):
    def __init__(self):
        self.subscriptions: Dict[str, List[Callable]] = {}
        self.logger = logging.getLogger("communication.inmemory")
    
    async def publish(self, channel: str, message: Message) -> None:
        if channel in self.subscriptions:
            for handler in self.subscriptions[channel]:
                try:
                    if inspect.iscoroutinefunction(handler):
                        await handler(message)
                    else:
                        handler(message)
                except Exception as e:
                    self.logger.error(f"Handler error: {e}")
    
    async def subscribe(self, channel: str, handler: Callable[[Message], None]) -> None:
        if channel not in self.subscriptions:
            self.subscriptions[channel] = []
        self.subscriptions[channel].append(handler)
    
    async def unsubscribe(self, channel: str) -> None:
        if channel in self.subscriptions:
            del self.subscriptions[channel]


class CommunicationProtocol:
    def __init__(self, agent_id: str, message_bus: MessageBus):
        self.agent_id = agent_id
        self.message_bus = message_bus
        self.logger = logging.getLogger(f"communication.{agent_id}")
        self.message_handlers: Dict[MessageType, List[Callable]] = {}
        self.pending_requests: Dict[str, asyncio.Future] = {}
        
        self._setup_default_channels()
    
    def _setup_default_channels(self):
        channels = [
            f"agent.{self.agent_id}",
            "broadcast",
            "system.commands",
            "coordination"
        ]
        
        for channel in channels:
            asyncio.create_task(
                self.message_bus.subscribe(channel, self._handle_message)
            )
    
    async def send_message(
        self,
        recipient_id: str,
        message_type: MessageType,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None
    ) -> str:
        message = Message(
            id=str(uuid.uuid4()),
            sender_id=self.agent_id,
            recipient_id=recipient_id,
            message_type=message_type,
            payload=payload,
            timestamp=datetime.now(timezone.utc),
            correlation_id=correlation_id
        )
        
        if recipient_id == "broadcast":
            channel = "broadcast"
        else:
            channel = f"agent.{recipient_id}"
        
        await self.message_bus.publish(channel, message)
        self.logger.debug(f"Sent message {message.id} to {recipient_id}")
        
        return message.id
    
    async def send_request(
        self,
        recipient_id: str,
        message_type: MessageType,
        payload: Dict[str, Any],
        timeout: float = 30.0
    ) -> Message:
        correlation_id = str(uuid.uuid4())
        future = asyncio.Future()
        self.pending_requests[correlation_id] = future
        
        try:
            await self.send_message(
                recipient_id=recipient_id,
                message_type=message_type,
                payload=payload,
                correlation_id=correlation_id
            )
            
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
            
        finally:
            if correlation_id in self.pending_requests:
                del self.pending_requests[correlation_id]
    
    async def send_response(
        self,
        original_message: Message,
        response_payload: Dict[str, Any]
    ):
        await self.send_message(
            recipient_id=original_message.sender_id,
            message_type=MessageType.TASK_RESPONSE,
            payload=response_payload,
            correlation_id=original_message.correlation_id
        )
    
    async def broadcast_message(
        self,
        message_type: MessageType,
        payload: Dict[str, Any]
    ):
        await self.send_message("broadcast", message_type, payload)
    
    def register_handler(
        self,
        message_type: MessageType,
        handler: Callable[[Message], None]
    ):
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []
        self.message_handlers[message_type].append(handler)
    
    async def _handle_message(self, message: Message):
        if message.correlation_id and message.correlation_id in self.pending_requests:
            future = self.pending_requests[message.correlation_id]
            if not future.done():
                future.set_result(message)
            return
        
        if message.message_type in self.message_handlers:
            for handler in self.message_handlers[message.message_type]:
                try:
                    if inspect.iscoroutinefunction(handler):
                        await handler(message)
                    else:
                        handler(message)
                except Exception as e:
                    self.logger.error(f"Message handler error: {e}")
    
    async def send_heartbeat(self, status_info: Dict[str, Any]):
        await self.broadcast_message(
            MessageType.HEARTBEAT,
            {
                "agent_id": self.agent_id,
                "status": status_info,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )


class CoordinationService:
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.active_agents: Dict[str, Dict[str, Any]] = {}
        self.task_assignments: Dict[str, str] = {}
        self.logger = logging.getLogger("coordination.service")
        
        self.protocol = CommunicationProtocol("coordinator", message_bus)
        self.protocol.register_handler(
            MessageType.HEARTBEAT, 
            self._handle_agent_heartbeat
        )
        self.protocol.register_handler(
            MessageType.AGENT_STATUS,
            self._handle_agent_status
        )
    
    async def _handle_agent_heartbeat(self, message: Message):
        agent_id = message.payload.get("agent_id")
        status = message.payload.get("status")
        
        self.active_agents[agent_id] = {
            "last_seen": datetime.now(timezone.utc),
            "status": status,
            "sender_id": message.sender_id
        }
        
        self.logger.debug(f"Received heartbeat from agent {agent_id}")
    
    async def _handle_agent_status(self, message: Message):
        agent_id = message.sender_id
        status_info = message.payload
        
        if agent_id in self.active_agents:
            self.active_agents[agent_id].update(status_info)
    
    async def assign_task(
        self,
        task_id: str,
        task_data: Dict[str, Any],
        target_agent: str = None
    ) -> bool:
        if target_agent:
            if target_agent not in self.active_agents:
                return False
            selected_agent = target_agent
        else:
            selected_agent = self._select_best_agent(task_data)
            if not selected_agent:
                return False
        
        try:
            response = await self.protocol.send_request(
                recipient_id=selected_agent,
                message_type=MessageType.TASK_REQUEST,
                payload={
                    "task_id": task_id,
                    "task_data": task_data
                }
            )
            
            if response.payload.get("accepted"):
                self.task_assignments[task_id] = selected_agent
                return True
            
        except asyncio.TimeoutError:
            self.logger.warning(f"Task assignment timeout for agent {selected_agent}")
        
        return False
    
    def _select_best_agent(self, task_data: Dict[str, Any]) -> Optional[str]:
        if not self.active_agents:
            return None
        
        available_agents = [
            agent_id for agent_id, info in self.active_agents.items()
            if info.get("status", {}).get("state") == "running"
        ]
        
        if not available_agents:
            return None
        
        return min(
            available_agents,
            key=lambda agent_id: self.active_agents[agent_id].get("status", {}).get("queue_size", 0)
        )
    
    async def broadcast_system_command(self, command: str, params: Dict[str, Any]):
        await self.protocol.broadcast_message(
            MessageType.SYSTEM_COMMAND,
            {
                "command": command,
                "params": params
            }
        )
    
    def get_swarm_status(self) -> Dict[str, Any]:
        return {
            "active_agents": len(self.active_agents),
            "agents": self.active_agents,
            "task_assignments": self.task_assignments
        }