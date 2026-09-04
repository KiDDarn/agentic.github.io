import asyncio
import inspect
import aiohttp
import httpx
import logging
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass
from abc import ABC, abstractmethod
import json
from urllib.parse import urljoin
from datetime import datetime, timezone


@dataclass
class APIEndpoint:
    name: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = None
    auth_type: str = None
    rate_limit: int = None
    timeout: float = 30.0
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {}


@dataclass
class APIResponse:
    status_code: int
    data: Any
    headers: Dict[str, str]
    endpoint: str
    timestamp: datetime
    error: Optional[str] = None


class APIClient(ABC):
    def __init__(self, base_url: str, name: str):
        self.base_url = base_url
        self.name = name
        self.endpoints: Dict[str, APIEndpoint] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(f"api.{name}")
        self.rate_limiters: Dict[str, RateLimiter] = {}
        
    @abstractmethod
    async def authenticate(self) -> bool:
        pass
    
    async def initialize(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        await self.authenticate()
    
    async def cleanup(self):
        if self.session:
            await self.session.close()
    
    def register_endpoint(self, endpoint: APIEndpoint):
        self.endpoints[endpoint.name] = endpoint
        if endpoint.rate_limit:
            self.rate_limiters[endpoint.name] = RateLimiter(endpoint.rate_limit)
    
    async def call_endpoint(
        self, 
        endpoint_name: str, 
        params: Dict[str, Any] = None,
        data: Dict[str, Any] = None
    ) -> APIResponse:
        if endpoint_name not in self.endpoints:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")
        
        endpoint = self.endpoints[endpoint_name]
        
        if endpoint_name in self.rate_limiters:
            await self.rate_limiters[endpoint_name].acquire()
        
        url = urljoin(self.base_url, endpoint.url)
        
        try:
            async with self.session.request(
                method=endpoint.method,
                url=url,
                params=params,
                json=data,
                headers=endpoint.headers,
                timeout=endpoint.timeout
            ) as response:
                response_data = None
                try:
                    response_data = await response.json()
                except:
                    response_data = await response.text()
                
                return APIResponse(
                    status_code=response.status,
                    data=response_data,
                    headers=dict(response.headers),
                    endpoint=endpoint_name,
                    timestamp=datetime.now(timezone.utc),
                    error=None if response.status < 400 else str(response_data)
                )
                
        except Exception as e:
            self.logger.error(f"API call failed for {endpoint_name}: {e}")
            return APIResponse(
                status_code=0,
                data=None,
                headers={},
                endpoint=endpoint_name,
                timestamp=datetime.now(timezone.utc),
                error=str(e)
            )


class HTTPAPIClient(APIClient):
    def __init__(self, base_url: str, name: str, api_key: str = None):
        super().__init__(base_url, name)
        self.api_key = api_key
    
    async def authenticate(self) -> bool:
        if self.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            })
        return True


class GraphQLAPIClient(APIClient):
    def __init__(self, base_url: str, name: str, api_key: str = None):
        super().__init__(base_url, name)
        self.api_key = api_key
    
    async def authenticate(self) -> bool:
        if self.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            })
        return True
    
    async def query(self, query: str, variables: Dict[str, Any] = None) -> APIResponse:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        return await self.call_endpoint("graphql", data=payload)


class WebSocketAPIClient:
    def __init__(self, url: str, name: str):
        self.url = url
        self.name = name
        self.websocket = None
        self.logger = logging.getLogger(f"ws.{name}")
        self.message_handlers: List[Callable] = []
        
    async def connect(self):
        import websockets
        self.websocket = await websockets.connect(self.url)
        asyncio.create_task(self._listen())
    
    async def disconnect(self):
        if self.websocket:
            await self.websocket.close()
    
    async def send_message(self, message: Dict[str, Any]):
        if self.websocket:
            await self.websocket.send(json.dumps(message))
    
    def add_message_handler(self, handler: Callable):
        self.message_handlers.append(handler)
    
    async def _listen(self):
        while self.websocket:
            try:
                message = await self.websocket.recv()
                data = json.loads(message)
                for handler in self.message_handlers:
                    if inspect.iscoroutinefunction(handler):
                        await handler(data)
                    else:
                        handler(data)
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}")
                break


class APIRegistry:
    def __init__(self):
        self.clients: Dict[str, APIClient] = {}
        self.logger = logging.getLogger("api.registry")
    
    async def register_client(self, client: APIClient):
        await client.initialize()
        self.clients[client.name] = client
        self.logger.info(f"Registered API client: {client.name}")
    
    async def unregister_client(self, name: str):
        if name in self.clients:
            await self.clients[name].cleanup()
            del self.clients[name]
    
    def get_client(self, name: str) -> Optional[APIClient]:
        return self.clients.get(name)
    
    async def call_api(
        self, 
        client_name: str, 
        endpoint_name: str, 
        **kwargs
    ) -> APIResponse:
        client = self.get_client(client_name)
        if not client:
            raise ValueError(f"Unknown API client: {client_name}")
        
        return await client.call_endpoint(endpoint_name, **kwargs)
    
    async def cleanup_all(self):
        for client in self.clients.values():
            await client.cleanup()
        self.clients.clear()


class RateLimiter:
    def __init__(self, max_requests: int, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = datetime.now(timezone.utc).timestamp()
            
            self.requests = [
                req_time for req_time in self.requests
                if now - req_time < self.time_window
            ]
            
            if len(self.requests) >= self.max_requests:
                sleep_time = self.time_window - (now - self.requests[0])
                await asyncio.sleep(sleep_time)
                return await self.acquire()
            
            self.requests.append(now)


class APIStackManager:
    def __init__(self):
        self.registry = APIRegistry()
        self.logger = logging.getLogger("api.stack")
    
    async def setup_common_apis(self):
        """Setup commonly used APIs"""
        
        # Example REST API
        rest_client = HTTPAPIClient("https://api.example.com", "example_rest")
        rest_client.register_endpoint(
            APIEndpoint("users", "/users", "GET", rate_limit=100)
        )
        rest_client.register_endpoint(
            APIEndpoint("create_user", "/users", "POST", rate_limit=50)
        )
        await self.registry.register_client(rest_client)
        
        # Example GraphQL API
        graphql_client = GraphQLAPIClient("https://api.github.com/graphql", "github")
        graphql_client.register_endpoint(
            APIEndpoint("graphql", "/graphql", "POST", rate_limit=5000)
        )
        await self.registry.register_client(graphql_client)
    
    async def add_custom_api(
        self, 
        name: str, 
        base_url: str, 
        api_key: str = None,
        endpoints: List[APIEndpoint] = None
    ):
        client = HTTPAPIClient(base_url, name, api_key)
        
        if endpoints:
            for endpoint in endpoints:
                client.register_endpoint(endpoint)
        
        await self.registry.register_client(client)
    
    def get_registry(self) -> APIRegistry:
        return self.registry