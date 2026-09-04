import asyncio
import inspect
import json
import logging
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import psutil
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse


@dataclass
class SystemMetrics:
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_io: Dict[str, int]
    active_processes: int


@dataclass
class AgentMetrics:
    agent_id: str
    timestamp: datetime
    state: str
    tasks_completed: int
    tasks_failed: int
    avg_execution_time: float
    queue_size: int
    memory_usage: float
    cpu_usage: float


class MetricsCollector:
    def __init__(self, collection_interval: float = 5.0):
        self.collection_interval = collection_interval
        self.system_metrics: deque = deque(maxlen=1000)
        self.agent_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.is_collecting = False
        self._collection_task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger("monitoring.collector")

    async def start_collection(self):
        self.is_collecting = True
        self._collection_task = asyncio.create_task(self._collect_metrics())
        self.logger.info("Started metrics collection")

    async def stop_collection(self):
        self.is_collecting = False
        if self._collection_task and not self._collection_task.done():
            self._collection_task.cancel()
        self.logger.info("Stopped metrics collection")

    async def _collect_metrics(self):
        while self.is_collecting:
            try:
                system_metrics = await self._collect_system_metrics()
                self.system_metrics.append(system_metrics)

                await asyncio.sleep(self.collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error collecting metrics: {e}")

    async def _collect_system_metrics(self) -> SystemMetrics:
        cpu_usage = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        network = psutil.net_io_counters()

        return SystemMetrics(
            timestamp=datetime.now(timezone.utc),
            cpu_usage=cpu_usage,
            memory_usage=memory.percent,
            disk_usage=disk.percent,
            network_io={"bytes_sent": network.bytes_sent, "bytes_recv": network.bytes_recv},
            active_processes=len(psutil.pids()),
        )

    def add_agent_metrics(self, agent_metrics: AgentMetrics):
        self.agent_metrics[agent_metrics.agent_id].append(agent_metrics)

    def get_system_metrics(self, limit: int = 100) -> List[SystemMetrics]:
        return list(self.system_metrics)[-limit:]

    def get_agent_metrics(self, agent_id: str, limit: int = 100) -> List[AgentMetrics]:
        return list(self.agent_metrics[agent_id])[-limit:]

    def get_latest_system_metrics(self) -> Optional[SystemMetrics]:
        return self.system_metrics[-1] if self.system_metrics else None

    def get_all_agent_metrics(self) -> Dict[str, List[AgentMetrics]]:
        return {agent_id: list(metrics) for agent_id, metrics in self.agent_metrics.items()}


class AlertManager:
    def __init__(self):
        self.alert_rules: List[AlertRule] = []
        self.active_alerts: List[Alert] = []
        self.alert_handlers: List[Callable] = []
        self.logger = logging.getLogger("monitoring.alerts")

    def add_alert_rule(self, rule: "AlertRule"):
        self.alert_rules.append(rule)
        self.logger.info(f"Added alert rule: {rule.name}")

    def add_alert_handler(self, handler: Callable):
        self.alert_handlers.append(handler)

    async def check_alerts(self, system_metrics: SystemMetrics, agent_metrics: Dict[str, AgentMetrics]):
        for rule in self.alert_rules:
            try:
                if await rule.evaluate(system_metrics, agent_metrics):
                    alert = Alert(
                        id=f"alert_{datetime.now(timezone.utc).timestamp()}",
                        rule_name=rule.name,
                        severity=rule.severity,
                        message=rule.message,
                        timestamp=datetime.now(timezone.utc),
                        resolved=False,
                    )

                    self.active_alerts.append(alert)
                    await self._trigger_alert(alert)

            except Exception as e:
                self.logger.error(f"Error evaluating alert rule {rule.name}: {e}")

    async def _trigger_alert(self, alert: "Alert"):
        self.logger.warning(f"ALERT: {alert.message}")

        for handler in self.alert_handlers:
            try:
                if inspect.iscoroutinefunction(handler):
                    await handler(alert)
                else:
                    handler(alert)
            except Exception as e:
                self.logger.error(f"Alert handler error: {e}")

    def get_active_alerts(self) -> List["Alert"]:
        return [alert for alert in self.active_alerts if not alert.resolved]

    def resolve_alert(self, alert_id: str):
        for alert in self.active_alerts:
            if alert.id == alert_id:
                alert.resolved = True
                alert.resolved_at = datetime.now(timezone.utc)


@dataclass
class Alert:
    id: str
    rule_name: str
    severity: str
    message: str
    timestamp: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None


class AlertRule:
    def __init__(self, name: str, severity: str, message: str):
        self.name = name
        self.severity = severity
        self.message = message

    async def evaluate(self, system_metrics: SystemMetrics, agent_metrics: Dict[str, AgentMetrics]) -> bool:
        raise NotImplementedError


class CPUAlertRule(AlertRule):
    def __init__(self, threshold: float = 80.0):
        super().__init__(name="High CPU Usage", severity="warning", message=f"CPU usage exceeded {threshold}%")
        self.threshold = threshold

    async def evaluate(self, system_metrics: SystemMetrics, agent_metrics: Dict[str, AgentMetrics]) -> bool:
        return system_metrics.cpu_usage > self.threshold


class MemoryAlertRule(AlertRule):
    def __init__(self, threshold: float = 85.0):
        super().__init__(name="High Memory Usage", severity="warning", message=f"Memory usage exceeded {threshold}%")
        self.threshold = threshold

    async def evaluate(self, system_metrics: SystemMetrics, agent_metrics: Dict[str, AgentMetrics]) -> bool:
        return system_metrics.memory_usage > self.threshold


class AgentFailureAlertRule(AlertRule):
    def __init__(self, failure_threshold: int = 10):
        super().__init__(
            name="Agent High Failure Rate", severity="critical", message="Agent failure rate exceeded threshold"
        )
        self.failure_threshold = failure_threshold

    async def evaluate(self, system_metrics: SystemMetrics, agent_metrics: Dict[str, AgentMetrics]) -> bool:
        for agent_id, metrics in agent_metrics.items():
            total_tasks = metrics.tasks_completed + metrics.tasks_failed
            if total_tasks > 0 and metrics.tasks_failed > self.failure_threshold:
                return True
        return False


class MonitoringDashboard:
    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager):
        self.metrics_collector = metrics_collector
        self.alert_manager = alert_manager
        self.app = FastAPI()
        self.websocket_connections: List[WebSocket] = []
        self.logger = logging.getLogger("monitoring.dashboard")

        self._setup_routes()

    def _setup_routes(self):
        @self.app.get("/")
        async def dashboard():
            return HTMLResponse(self._get_dashboard_html())

        @self.app.get("/api/metrics/system")
        async def get_system_metrics():
            metrics = self.metrics_collector.get_system_metrics()
            return [asdict(m) for m in metrics]

        @self.app.get("/api/metrics/agents")
        async def get_agent_metrics():
            metrics = self.metrics_collector.get_all_agent_metrics()
            return {agent_id: [asdict(m) for m in agent_metrics] for agent_id, agent_metrics in metrics.items()}

        @self.app.get("/api/alerts")
        async def get_alerts():
            alerts = self.alert_manager.get_active_alerts()
            return [asdict(alert) for alert in alerts]

        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self.websocket_connections.append(websocket)

            try:
                while True:
                    await websocket.receive_text()
            except WebSocketDisconnect:
                self.websocket_connections.remove(websocket)

    async def broadcast_update(self, data: Dict[str, Any]):
        disconnected = []

        for connection in self.websocket_connections:
            try:
                await connection.send_text(json.dumps(data))
            except Exception:
                disconnected.append(connection)

        for connection in disconnected:
            self.websocket_connections.remove(connection)

    def _get_dashboard_html(self) -> str:
        return """
<!DOCTYPE html>
<html>
<head>
    <title>Agentic OS Monitoring Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; margin-bottom: 30px; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric-title { font-size: 18px; font-weight: bold; margin-bottom: 10px; }
        .metric-value { font-size: 24px; color: #007bff; }
        .alerts-section { margin-top: 30px; }
        .alert { padding: 10px; margin: 5px 0; border-radius: 4px; }
        .alert-warning { background: #fff3cd; border-left: 4px solid #ffc107; }
        .alert-critical { background: #f8d7da; border-left: 4px solid #dc3545; }
        canvas { max-height: 300px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Agentic OS Monitoring Dashboard</h1>
        </div>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-title">System CPU Usage</div>
                <div class="metric-value" id="cpu-usage">0%</div>
                <canvas id="cpu-chart"></canvas>
            </div>

            <div class="metric-card">
                <div class="metric-title">Memory Usage</div>
                <div class="metric-value" id="memory-usage">0%</div>
                <canvas id="memory-chart"></canvas>
            </div>

            <div class="metric-card">
                <div class="metric-title">Active Agents</div>
                <div class="metric-value" id="active-agents">0</div>
            </div>

            <div class="metric-card">
                <div class="metric-title">Total Tasks</div>
                <div class="metric-value" id="total-tasks">0</div>
            </div>
        </div>

        <div class="alerts-section">
            <h2>Active Alerts</h2>
            <div id="alerts-container"></div>
        </div>
    </div>

    <script>
        const ws = new WebSocket(`ws://${window.location.host}/ws`);

        // Initialize charts
        const cpuChart = new Chart(document.getElementById('cpu-chart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'CPU %',
                    data: [],
                    borderColor: '#007bff',
                    tension: 0.1
                }]
            },
            options: { responsive: true, scales: { y: { beginAtZero: true, max: 100 } } }
        });

        const memoryChart = new Chart(document.getElementById('memory-chart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Memory %',
                    data: [],
                    borderColor: '#28a745',
                    tension: 0.1
                }]
            },
            options: { responsive: true, scales: { y: { beginAtZero: true, max: 100 } } }
        });

        // Fetch and update data
        async function updateDashboard() {
            try {
                const systemMetrics = await fetch('/api/metrics/system').then(r => r.json());
                const agentMetrics = await fetch('/api/metrics/agents').then(r => r.json());
                const alerts = await fetch('/api/alerts').then(r => r.json());

                if (systemMetrics.length > 0) {
                    const latest = systemMetrics[systemMetrics.length - 1];
                    document.getElementById('cpu-usage').textContent = latest.cpu_usage.toFixed(1) + '%';
                    document.getElementById('memory-usage').textContent = latest.memory_usage.toFixed(1) + '%';

                    // Update charts
                    const labels = systemMetrics.slice(-20).map(m => new Date(m.timestamp).toLocaleTimeString());
                    const cpuData = systemMetrics.slice(-20).map(m => m.cpu_usage);
                    const memoryData = systemMetrics.slice(-20).map(m => m.memory_usage);

                    cpuChart.data.labels = labels;
                    cpuChart.data.datasets[0].data = cpuData;
                    cpuChart.update();

                    memoryChart.data.labels = labels;
                    memoryChart.data.datasets[0].data = memoryData;
                    memoryChart.update();
                }

                // Update agent metrics
                const agentCount = Object.keys(agentMetrics).length;
                document.getElementById('active-agents').textContent = agentCount;

                let totalTasks = 0;
                Object.values(agentMetrics).forEach(metrics => {
                    if (metrics.length > 0) {
                        const latest = metrics[metrics.length - 1];
                        totalTasks += latest.tasks_completed;
                    }
                });
                document.getElementById('total-tasks').textContent = totalTasks;

                // Update alerts
                const alertsContainer = document.getElementById('alerts-container');
                alertsContainer.innerHTML = '';

                alerts.forEach(alert => {
                    const alertDiv = document.createElement('div');
                    alertDiv.className = `alert alert-${alert.severity}`;
                    alertDiv.innerHTML = `
                        <strong>${alert.rule_name}</strong><br>
                        ${alert.message}<br>
                        <small>${new Date(alert.timestamp).toLocaleString()}</small>
                    `;
                    alertsContainer.appendChild(alertDiv);
                });

            } catch (error) {
                console.error('Error updating dashboard:', error);
            }
        }

        // Update every 5 seconds
        setInterval(updateDashboard, 5000);
        updateDashboard();
    </script>
</body>
</html>
        """

    async def start_server(self, host: str = "0.0.0.0", port: int = 8080):
        config = uvicorn.Config(app=self.app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()


class MonitoringService:
    def __init__(self):
        self.metrics_collector = MetricsCollector()
        self.alert_manager = AlertManager()
        self.dashboard = MonitoringDashboard(self.metrics_collector, self.alert_manager)
        self.logger = logging.getLogger("monitoring.service")

        # Setup default alert rules
        self.alert_manager.add_alert_rule(CPUAlertRule(80.0))
        self.alert_manager.add_alert_rule(MemoryAlertRule(85.0))
        self.alert_manager.add_alert_rule(AgentFailureAlertRule(10))

    async def start(self, dashboard_port: int = 8080):
        await self.metrics_collector.start_collection()
        self.logger.info("Monitoring service started")

        # Start dashboard in background
        asyncio.create_task(self.dashboard.start_server(port=dashboard_port))

    async def stop(self):
        await self.metrics_collector.stop_collection()
        self.logger.info("Monitoring service stopped")

    async def report_agent_metrics(self, agent_metrics: AgentMetrics):
        self.metrics_collector.add_agent_metrics(agent_metrics)

        # Check alerts
        system_metrics = self.metrics_collector.get_latest_system_metrics()
        if system_metrics:
            await self.alert_manager.check_alerts(system_metrics, {agent_metrics.agent_id: agent_metrics})

        # Broadcast update to dashboard
        await self.dashboard.broadcast_update({"type": "agent_metrics", "data": asdict(agent_metrics)})
