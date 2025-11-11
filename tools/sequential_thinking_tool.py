from __future__ import annotations
from typing import Any, Dict, List, Optional, Literal
from dataclasses import dataclass, field, asdict
from datetime import datetime
import uuid
import json

from agno.tools import Toolkit

StepType = Literal["reason", "sql", "tool", "decision", "validate"]

@dataclass
class Step:
    index: int
    title: str
    kind: StepType = "reason"
    depends_on: List[int] = field(default_factory=list)
    inputs: Dict[str, Any] = field(default_factory=dict)
    expected: Optional[str] = None
    status: Literal["pending", "running", "done", "skipped", "failed"] = "pending"
    result: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

@dataclass
class Plan:
    plan_id: str
    created_at: str
    title: str
    goal: str
    constraints: List[str] = field(default_factory=list)
    max_steps: int = 20
    steps: List[Step] = field(default_factory=list)
    reflections: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

class SequentialThinkingTools(Toolkit):
    """
    A deterministic planning scratchpad for Agents:
    - new_plan: create a plan with goal/constraints
    - add_step: append a sequenced step
    - set_step_status / record_result: lifecycle ops
    - reflect: append reflection note
    - get_plan: fetch as JSON
    """
    def __init__(self, name: str = "sequential_thinking", **kwargs):
        registered_tools = [
            self.new_plan,
            self.add_step,
            self.set_step_status,
            self.record_result,
            self.reflect,
            self.get_plan,
        ]
        for tool_func in registered_tools:
            if not hasattr(tool_func, "__name__") and hasattr(tool_func, "name"):
                setattr(tool_func, "__name__", getattr(tool_func, "name"))
        super().__init__(name=name, tools=registered_tools, **kwargs)
        self._plans: Dict[str, Plan] = {}

    def new_plan(self, title: str, goal: str,
                 constraints: Optional[List[str]] = None,
                 max_steps: int = 20,
                 metadata: Optional[Dict[str, Any]] = None) -> str:
        """Start a new plan with a title, goal, and optional constraints."""
        pid = str(uuid.uuid4())
        plan = Plan(
            plan_id=pid,
            created_at=datetime.utcnow().isoformat(),
            title=title,
            goal=goal,
            constraints=constraints or [],
            max_steps=max_steps,
            meta=metadata or {}
        )
        self._plans[pid] = plan
        return json.dumps({"ok": True, "plan_id": pid, "plan": asdict(plan)})

    def add_step(self, plan_id: str, title: str, kind: StepType = "reason",
                 depends_on: Optional[List[int]] = None,
                 inputs: Optional[Dict[str, Any]] = None,
                 expected: Optional[str] = None) -> str:
        """Append a step to an existing plan."""
        plan = self._require(plan_id)
        if len(plan.steps) >= plan.max_steps:
            return json.dumps({"ok": False, "error": "max_steps_exceeded"})
        step = Step(
            index=len(plan.steps),
            title=title,
            kind=kind,
            depends_on=depends_on or [],
            inputs=inputs or {},
            expected=expected
        )
        plan.steps.append(step)
        return json.dumps({"ok": True, "step": asdict(step)})

    def set_step_status(self, plan_id: str, index: int,
                        status: Literal["pending","running","done","skipped","failed"]) -> str:
        """Update a step's status (pending|running|done|skipped|failed)."""
        plan = self._require(plan_id)
        try:
            step = plan.steps[index]
        except IndexError:
            return json.dumps({"ok": False, "error": "invalid_step_index"})
        now = datetime.utcnow().isoformat()
        if status == "running":
            step.started_at = now
        if status in ("done","skipped","failed"):
            step.finished_at = now
        step.status = status
        return json.dumps({"ok": True, "step": asdict(step)})

    def record_result(self, plan_id: str, index: int, result: str, success: bool = True) -> str:
        """Record a result for a step; auto-completes when success=True."""
        plan = self._require(plan_id)
        try:
            step = plan.steps[index]
        except IndexError:
            return json.dumps({"ok": False, "error": "invalid_step_index"})
        step.result = result
        if success and step.status != "done":
            step.status = "done"
            step.finished_at = datetime.utcnow().isoformat()
        return json.dumps({"ok": True, "step": asdict(step)})

    def reflect(self, plan_id: str, note: str) -> str:
        """Append a free-form reflection note to the plan."""
        plan = self._require(plan_id)
        plan.reflections.append(note)
        return json.dumps({"ok": True, "reflections": plan.reflections})

    def get_plan(self, plan_id: str) -> str:
        """Return the entire plan JSON."""
        plan = self._require(plan_id)
        return json.dumps({"ok": True, "plan": asdict(plan)})

    # ---- helpers ----
    def _require(self, plan_id: str) -> Plan:
        if plan_id not in self._plans:
            raise ValueError(f"Unknown plan_id: {plan_id}")
        return self._plans[plan_id]
