import asyncio
import logging
import time
from typing import Any, Dict, List

from core.agent_base import AgentCapability, BaseAgent, Task
from core.api_integration import APIStackManager
from core.communication import CoordinationService, InMemoryMessageBus, RedisMessageBus
from core.monitoring import MonitoringService
from core.parallel_engine import ParallelExecutionEngine
from core.swarm_orchestrator import SwarmOrchestrator


class AgenticOS:
    def __init__(
        self,
        use_redis: bool = False,
        redis_url: str = "redis://localhost:6379",
        max_agents: int = 100,
        max_threads: int = 50,
        dashboard_port: int = 8080,
    ):
        self.logger = logging.getLogger("agentic_os")

        # Initialize core components
        self.swarm_orchestrator = SwarmOrchestrator(max_agents, max_threads)
        self.api_stack_manager = APIStackManager()
        self.parallel_engine = ParallelExecutionEngine(max_threads=max_threads)

        # Initialize communication
        if use_redis:
            self.message_bus = RedisMessageBus(redis_url)
        else:
            self.message_bus = InMemoryMessageBus()

        self.coordination_service = CoordinationService(self.message_bus)
        self.monitoring_service = MonitoringService()

        self.is_running = False
        self.dashboard_port = dashboard_port

    async def initialize(self):
        """Initialize the Agentic OS"""
        self.logger.info("Initializing Agentic OS...")

        # Initialize message bus if Redis
        if hasattr(self.message_bus, "initialize"):
            await self.message_bus.initialize()

        # Setup common APIs
        await self.api_stack_manager.setup_common_apis()

        # Start monitoring
        await self.monitoring_service.start(self.dashboard_port)

        self.logger.info("Agentic OS initialized successfully")

    async def start(self):
        """Start the Agentic OS"""
        if self.is_running:
            self.logger.warning("Agentic OS is already running")
            return

        await self.initialize()
        self.is_running = True

        # Start swarm orchestrator
        asyncio.create_task(self.swarm_orchestrator.start_swarm())

        self.logger.info("Agentic OS started successfully")
        self.logger.info(f"Monitoring dashboard available at http://localhost:{self.dashboard_port}")

    async def stop(self):
        """Stop the Agentic OS"""
        if not self.is_running:
            self.logger.warning("Agentic OS is not running")
            return

        self.logger.info("Stopping Agentic OS...")

        # Stop components
        await self.swarm_orchestrator.stop_swarm()
        await self.parallel_engine.shutdown()
        await self.monitoring_service.stop()

        # Cleanup message bus
        if hasattr(self.message_bus, "cleanup"):
            await self.message_bus.cleanup()

        # Cleanup API clients
        await self.api_stack_manager.registry.cleanup_all()

        self.is_running = False
        self.logger.info("Agentic OS stopped")

    async def register_agent(self, agent: BaseAgent) -> bool:
        """Register a new agent with the system"""
        success = await self.swarm_orchestrator.register_agent(agent)

        if success:
            # Setup agent with API access
            registry = self.api_stack_manager.get_registry()
            for client_name, client in registry.clients.items():
                agent.add_api_client(client_name, client)

            # Start the agent
            await self.swarm_orchestrator.start_agent(agent.id)

            self.logger.info(f"Agent {agent.name} registered and started")

        return success

    async def create_custom_agent(
        self,
        name: str,
        capabilities: List[Dict[str, Any]],
        execute_func: callable,
        initialize_func: callable = None,
        cleanup_func: callable = None,
    ) -> BaseAgent:
        """Create a custom agent with specified capabilities"""

        class CustomAgent(BaseAgent):
            def __init__(self):
                super().__init__(name=name)
                self.execute_func = execute_func
                self.initialize_func = initialize_func
                self.cleanup_func = cleanup_func

                # Register capabilities
                for cap_config in capabilities:
                    capability = AgentCapability(
                        name=cap_config["name"],
                        description=cap_config["description"],
                        input_schema=cap_config.get("input_schema", {}),
                        output_schema=cap_config.get("output_schema", {}),
                    )
                    self.register_capability(capability)

            async def initialize(self):
                if self.initialize_func:
                    await self.initialize_func(self)

            async def execute_task(self, task: Task) -> Dict[str, Any]:
                return await self.execute_func(self, task)

            async def cleanup(self):
                if self.cleanup_func:
                    await self.cleanup_func(self)

        return CustomAgent()

    async def submit_task(self, task_data: Dict[str, Any], task_type: str = "general") -> str:
        """Submit a task to the swarm"""
        task = Task(id=f"task_{time.time()}", agent_id="", task_type=task_type, payload=task_data)

        success = await self.swarm_orchestrator.submit_task(task)

        if success:
            self.logger.info(f"Task {task.id} submitted successfully")
            return task.id
        else:
            self.logger.error(f"Failed to submit task {task.id}")
            return None

    async def add_api_integration(
        self, name: str, base_url: str, api_key: str = None, endpoints: List[Dict[str, Any]] = None
    ):
        """Add a new API integration"""
        from core.api_integration import APIEndpoint

        endpoint_objects = []
        if endpoints:
            for ep in endpoints:
                endpoint_objects.append(APIEndpoint(**ep))

        await self.api_stack_manager.add_custom_api(name, base_url, api_key, endpoint_objects)

        self.logger.info(f"Added API integration: {name}")

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        return {
            "is_running": self.is_running,
            "swarm_status": self.swarm_orchestrator.get_swarm_status(),
            "parallel_engine_status": self.parallel_engine.get_status(),
            "coordination_status": self.coordination_service.get_swarm_status(),
            "dashboard_url": f"http://localhost:{self.dashboard_port}",
        }

    async def execute_parallel_tasks(self, tasks: List[Dict[str, Any]], mode: str = "async") -> List[Any]:
        """Execute multiple tasks in parallel using the parallel engine"""
        from core.parallel_engine import ExecutionMode

        mode_mapping = {"async": ExecutionMode.ASYNC, "thread": ExecutionMode.THREAD, "process": ExecutionMode.PROCESS}

        execution_mode = mode_mapping.get(mode, ExecutionMode.ASYNC)

        task_configs = []
        for task in tasks:
            task_configs.append({**task, "mode": execution_mode})

        task_ids = await self.parallel_engine.submit_batch(task_configs)
        results = await self.parallel_engine.wait_for_all(task_ids)

        return [result.result for result in results]


# Example usage and demo agents
class DataProcessingAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="DataProcessor")

        # Register capabilities
        self.register_capability(
            AgentCapability(
                name="data_analysis",
                description="Analyze data and generate insights",
                input_schema={"data": "list", "analysis_type": "string"},
                output_schema={"insights": "dict", "summary": "string"},
            )
        )

        self.register_capability(
            AgentCapability(
                name="data_transformation",
                description="Transform data into different formats",
                input_schema={"data": "any", "target_format": "string"},
                output_schema={"transformed_data": "any"},
            )
        )

    async def initialize(self):
        self.logger.info("Data Processing Agent initialized")

    async def execute_task(self, task: Task) -> Dict[str, Any]:
        task_type = task.task_type
        payload = task.payload

        if task_type == "data_analysis":
            # Simulate data analysis
            data = payload.get("data", [])
            analysis_type = payload.get("analysis_type", "basic")

            # Mock analysis
            insights = {
                "total_items": len(data),
                "analysis_type": analysis_type,
                "timestamp": str(time.time()),
            }

            return {"insights": insights, "summary": f"Analyzed {len(data)} items using {analysis_type} analysis"}

        elif task_type == "data_transformation":
            data = payload.get("data")
            target_format = payload.get("target_format", "json")

            # Mock transformation
            transformed_data = {
                "original_data": data,
                "format": target_format,
                "transformed_at": str(time.time()),
            }

            return {"transformed_data": transformed_data}

        else:
            raise ValueError(f"Unsupported task type: {task_type}")

    async def cleanup(self):
        self.logger.info("Data Processing Agent cleanup completed")


class APIWorkerAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="APIWorker")

        self.register_capability(
            AgentCapability(
                name="api_request",
                description="Make API requests to external services",
                input_schema={"url": "string", "method": "string", "params": "dict"},
                output_schema={"response": "dict", "status_code": "int"},
            )
        )

    async def initialize(self):
        self.logger.info("API Worker Agent initialized")

    async def execute_task(self, task: Task) -> Dict[str, Any]:
        if task.task_type == "api_request":
            url = task.payload.get("url")
            method = task.payload.get("method", "GET")
            params = task.payload.get("params", {})

            # Use API client if available
            for client_name, client in self.api_clients.items():
                if url.startswith(client.base_url):
                    # Mock API call
                    return {
                        "response": {"message": f"API call to {url} successful"},
                        "status_code": 200,
                        "client_used": client_name,
                    }

            # Mock external API call
            return {
                "response": {"message": f"External API call to {url} successful"},
                "status_code": 200,
                "method": method,
                "params": params,
            }

        else:
            raise ValueError(f"Unsupported task type: {task.task_type}")

    async def cleanup(self):
        self.logger.info("API Worker Agent cleanup completed")


async def main():
    """Demo function showing how to use the Agentic OS"""

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Create and start the Agentic OS
    os = AgenticOS(use_redis=False, dashboard_port=8080)

    try:
        await os.start()

        # Create and register agents
        data_agent = DataProcessingAgent()
        api_agent = APIWorkerAgent()

        await os.register_agent(data_agent)
        await os.register_agent(api_agent)

        # Submit some demo tasks
        _task1_id = await os.submit_task({"data": [1, 2, 3, 4, 5], "analysis_type": "statistical"}, "data_analysis")

        _task2_id = await os.submit_task(
            {"url": "https://api.example.com/data", "method": "GET", "params": {"limit": 10}}, "api_request"
        )

        # Demo parallel execution
        parallel_tasks = [{"func": lambda x: x * 2, "args": (i,)} for i in range(5)]

        parallel_results = await os.execute_parallel_tasks(parallel_tasks)

        print("System Status:")
        print(os.get_system_status())

        print(f"\nParallel execution results: {parallel_results}")

        # Keep running for demo
        print("\nAgentic OS is running. Press Ctrl+C to stop.")
        print("Visit http://localhost:8080 for the monitoring dashboard")

        while True:
            await asyncio.sleep(10)
            status = os.get_system_status()
            print(f"Active agents: {status['swarm_status']['metrics'].total_agents}")

    except KeyboardInterrupt:
        print("\nShutting down Agentic OS...")

    finally:
        await os.stop()


if __name__ == "__main__":
    asyncio.run(main())
