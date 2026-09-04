import asyncio

import pytest

from agentic_os import AgenticOS, DataProcessingAgent
from core.agent_base import Task


@pytest.mark.asyncio
async def test_agentic_os_initialization_and_status():
    os = AgenticOS(use_redis=False, dashboard_port=8089)
    status = os.get_system_status()
    assert status["is_running"] is False
    assert "swarm_status" in status
    assert "parallel_engine_status" in status
    assert "coordination_status" in status
    assert status["dashboard_url"] == "http://localhost:8089"


@pytest.mark.asyncio
async def test_agentic_os_custom_agent_and_task_execution():
    os = AgenticOS(use_redis=False, dashboard_port=8090)

    # Test creating custom agent
    async def custom_execute(agent, task: Task):
        return {"processed": task.payload.get("value", 0) * 3}

    capabilities = [
        {
            "name": "multiply_three",
            "description": "Multiplies value by 3",
            "input_schema": {"value": "int"},
            "output_schema": {"processed": "int"},
        }
    ]

    agent = await os.create_custom_agent(
        name="MultiplierAgent",
        capabilities=capabilities,
        execute_func=custom_execute,
    )
    assert len(agent.capabilities) == 1
    assert agent.capabilities[0].name == "multiply_three"

    # Register agent with OS
    registered = await os.register_agent(agent)
    assert registered is True
    assert agent.id in os.swarm_orchestrator.agents
    await asyncio.sleep(0.02)

    # Submit task
    task_id = await os.submit_task({"value": 7}, task_type="multiply_three")
    assert task_id is not None
    await asyncio.sleep(0.05)

    assert agent.metrics.tasks_completed == 1

    # Cleanup
    await os.stop()


@pytest.mark.asyncio
async def test_agentic_os_parallel_tasks():
    os = AgenticOS(use_redis=False, dashboard_port=8091)

    def square(x):
        return x * x

    tasks = [{"func": square, "args": (i,)} for i in range(4)]
    results = await os.execute_parallel_tasks(tasks, mode="thread")
    assert results == [0, 1, 4, 9]

    await os.stop()


@pytest.mark.asyncio
async def test_agentic_os_demo_agents():
    data_agent = DataProcessingAgent()
    assert len(data_agent.capabilities) == 2

    # Test analysis task
    task_analysis = Task(
        id="t-da-1",
        agent_id=data_agent.id,
        task_type="data_analysis",
        payload={"data": [10, 20, 30], "analysis_type": "sum"},
    )
    result_analysis = await data_agent.execute_task(task_analysis)
    assert "insights" in result_analysis
    assert result_analysis["insights"]["total_items"] == 3

    # Test transformation task
    task_transform = Task(
        id="t-da-2",
        agent_id=data_agent.id,
        task_type="data_transformation",
        payload={"data": "sample", "target_format": "xml"},
    )
    result_transform = await data_agent.execute_task(task_transform)
    assert result_transform["transformed_data"]["format"] == "xml"

    # Test unsupported task
    task_bad = Task(id="t-da-3", agent_id=data_agent.id, task_type="unsupported", payload={})
    with pytest.raises(ValueError, match="Unsupported task type"):
        await data_agent.execute_task(task_bad)

    await data_agent.cleanup()
