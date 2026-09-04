import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from core.monitoring import (
    AgentFailureAlertRule,
    AgentMetrics,
    Alert,
    AlertManager,
    CPUAlertRule,
    MemoryAlertRule,
    MetricsCollector,
    MonitoringDashboard,
    SystemMetrics,
)


def test_metrics_dataclasses():
    now = datetime.now(timezone.utc)
    sys_m = SystemMetrics(
        timestamp=now,
        cpu_usage=15.5,
        memory_usage=45.0,
        disk_usage=60.0,
        network_io={"bytes_sent": 100, "bytes_recv": 200},
        active_processes=50,
    )
    assert sys_m.cpu_usage == 15.5
    assert sys_m.timestamp == now

    agent_m = AgentMetrics(
        agent_id="ag-1",
        timestamp=now,
        state="running",
        tasks_completed=10,
        tasks_failed=1,
        avg_execution_time=0.25,
        queue_size=0,
        memory_usage=20.0,
        cpu_usage=5.0,
    )
    assert agent_m.agent_id == "ag-1"
    assert agent_m.tasks_completed == 10


@pytest.mark.asyncio
async def test_metrics_collector_and_lifecycle():
    collector = MetricsCollector(collection_interval=0.01)

    # Collect system metrics directly
    sys_metric = await collector._collect_system_metrics()
    assert sys_metric.timestamp.tzinfo == timezone.utc
    assert 0 <= sys_metric.cpu_usage <= 100
    assert 0 <= sys_metric.memory_usage <= 100
    assert sys_metric.active_processes > 0

    # Add agent metrics
    now = datetime.now(timezone.utc)
    for i in range(5):
        collector.add_agent_metrics(
            AgentMetrics(
                agent_id="agent-x",
                timestamp=now,
                state="running",
                tasks_completed=i,
                tasks_failed=0,
                avg_execution_time=0.1,
                queue_size=0,
                memory_usage=10.0,
                cpu_usage=2.0,
            )
        )

    assert len(collector.get_agent_metrics("agent-x", limit=3)) == 3
    all_metrics = collector.get_all_agent_metrics()
    assert "agent-x" in all_metrics
    assert len(all_metrics["agent-x"]) == 5

    # Start and stop background collection
    await collector.start_collection()
    assert collector.is_collecting is True
    await asyncio.sleep(0.03)
    await collector.stop_collection()
    assert collector.is_collecting is False
    assert len(collector.system_metrics) > 0
    assert collector.get_latest_system_metrics() is not None


@pytest.mark.asyncio
async def test_alert_rules_evaluation():
    now = datetime.now(timezone.utc)
    sys_metrics = SystemMetrics(
        timestamp=now,
        cpu_usage=85.0,
        memory_usage=90.0,
        disk_usage=50.0,
        network_io={"bytes_sent": 0, "bytes_recv": 0},
        active_processes=10,
    )

    agent_metrics = {
        "ag-fail": AgentMetrics(
            agent_id="ag-fail",
            timestamp=now,
            state="running",
            tasks_completed=2,
            tasks_failed=15,
            avg_execution_time=1.0,
            queue_size=5,
            memory_usage=10.0,
            cpu_usage=5.0,
        ),
        "ag-ok": AgentMetrics(
            agent_id="ag-ok",
            timestamp=now,
            state="running",
            tasks_completed=20,
            tasks_failed=0,
            avg_execution_time=1.0,
            queue_size=0,
            memory_usage=10.0,
            cpu_usage=5.0,
        ),
    }

    cpu_rule = CPUAlertRule(threshold=80.0)
    assert await cpu_rule.evaluate(sys_metrics, agent_metrics) is True
    cpu_rule_high = CPUAlertRule(threshold=90.0)
    assert await cpu_rule_high.evaluate(sys_metrics, agent_metrics) is False

    mem_rule = MemoryAlertRule(threshold=85.0)
    assert await mem_rule.evaluate(sys_metrics, agent_metrics) is True
    mem_rule_high = MemoryAlertRule(threshold=95.0)
    assert await mem_rule_high.evaluate(sys_metrics, agent_metrics) is False

    fail_rule = AgentFailureAlertRule(failure_threshold=10)
    assert await fail_rule.evaluate(sys_metrics, agent_metrics) is True
    fail_rule_high = AgentFailureAlertRule(failure_threshold=20)
    assert await fail_rule_high.evaluate(sys_metrics, agent_metrics) is False


@pytest.mark.asyncio
async def test_alert_manager_and_handlers():
    alert_mgr = AlertManager()
    triggered_sync = []
    triggered_async = []

    def sync_handler(alert: Alert):
        triggered_sync.append(alert.id)

    async def async_handler(alert: Alert):
        triggered_async.append(alert.id)

    alert_mgr.add_alert_handler(sync_handler)
    alert_mgr.add_alert_handler(async_handler)

    rule = CPUAlertRule(threshold=50.0)
    alert_mgr.add_alert_rule(rule)

    now = datetime.now(timezone.utc)
    sys_metrics = SystemMetrics(
        timestamp=now,
        cpu_usage=75.0,
        memory_usage=40.0,
        disk_usage=40.0,
        network_io={"bytes_sent": 0, "bytes_recv": 0},
        active_processes=10,
    )

    await alert_mgr.check_alerts(sys_metrics, {})

    assert len(alert_mgr.active_alerts) == 1
    assert len(alert_mgr.get_active_alerts()) == 1
    assert len(triggered_sync) == 1
    assert len(triggered_async) == 1

    # Resolve alert
    alert_id = alert_mgr.active_alerts[0].id
    alert_mgr.resolve_alert(alert_id)
    assert len(alert_mgr.get_active_alerts()) == 0
    assert alert_mgr.active_alerts[0].resolved is True
    assert alert_mgr.active_alerts[0].resolved_at.tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_monitoring_dashboard_endpoints():
    collector = MetricsCollector()
    now = datetime.now(timezone.utc)
    collector.system_metrics.append(
        SystemMetrics(
            timestamp=now,
            cpu_usage=25.0,
            memory_usage=55.0,
            disk_usage=40.0,
            network_io={"bytes_sent": 100, "bytes_recv": 200},
            active_processes=20,
        )
    )
    collector.add_agent_metrics(
        AgentMetrics(
            agent_id="test-agent",
            timestamp=now,
            state="running",
            tasks_completed=5,
            tasks_failed=0,
            avg_execution_time=0.15,
            queue_size=0,
            memory_usage=15.0,
            cpu_usage=3.0,
        )
    )

    alert_mgr = AlertManager()
    alert_mgr.active_alerts.append(
        Alert(
            id="alert-1",
            rule_name="Test Rule",
            severity="warning",
            message="Test warning",
            timestamp=now,
        )
    )

    dashboard = MonitoringDashboard(collector, alert_mgr)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=dashboard.app), base_url="http://test") as client:
        # Dashboard HTML
        resp_root = await client.get("/")
        assert resp_root.status_code == 200
        assert "Agentic OS Monitoring Dashboard" in resp_root.text

        # System metrics API
        resp_sys = await client.get("/api/metrics/system")
        assert resp_sys.status_code == 200
        sys_json = resp_sys.json()
        assert len(sys_json) == 1
        assert sys_json[0]["cpu_usage"] == 25.0

        # Agent metrics API
        resp_ag = await client.get("/api/metrics/agents")
        assert resp_ag.status_code == 200
        ag_json = resp_ag.json()
        assert "test-agent" in ag_json
        assert ag_json["test-agent"][0]["tasks_completed"] == 5

        # Alerts API
        resp_al = await client.get("/api/alerts")
        assert resp_al.status_code == 200
        al_json = resp_al.json()
        assert len(al_json) == 1
        assert al_json[0]["id"] == "alert-1"

    # Broadcast update test
    await dashboard.broadcast_update({"test": 123})
