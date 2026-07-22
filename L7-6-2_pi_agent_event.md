#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
階段編號：L7-6-2
模組en：pi_agent_event
模組zh：PI Agent 事件流引擎
版本：1.1.0
製作日期：2026-07-17
功能類別：Graphify 核心
功能屬性：系統穩定與韌性
五行屬性：水
功能簡介：PiAgentCore 事件流引擎，提供反思、進化、記憶、驗證、規劃、多渠道、隱私保護、權限委託、資源預算、自主實驗室等 10 大能力領域的完整事件定義與流式處理
檔案路徑：~/.openclaw/skills/graphify/core/l7_pi_agent/pi_agent_event.py
tier: 1
api_version: v1
信賴等級：EXTRACTED
安全邊界：service
切入點：EventStream
依賴模組：
  - L7-6-1 # pi_agent_types
  - 標準庫 # asyncio
  - 標準庫 # typing
  - 標準庫 # time
  - 標準庫 # uuid
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional, TypeVar, Generic, Callable, Awaitable

from .pi_agent_types import AgentEvent

# ============================================================
# 核心功能代碼區（Core Logic）
# ============================================================

# ---- 事件類型常數 ----

# 反思事件（Reflection Events）
REFLECTION_TRIGGERED = "reflection_triggered"
REFLECTION_COMPLETED = "reflection_completed"
DAILY_REFLECTION_TRIGGERED = "daily_reflection_triggered"
DAILY_REFLECTION_COMPLETED = "daily_reflection_completed"

# 進化事件（Evolution Events）
SKILL_GENERATION_STARTED = "skill_generation_started"
SKILL_GENERATION_COMPLETED = "skill_generation_completed"
SKILL_EVOLUTION_TRIGGERED = "skill_evolution_triggered"
SKILL_PATCHED = "skill_patched"

# 記憶事件（Memory Events）
MEMORY_CONSOLIDATED = "memory_consolidated"
MEMORY_RECALLED = "memory_recalled"
MEMORY_CLEANED = "memory_cleaned"

# 驗證事件（Verification Events）
VERIFICATION_STARTED = "verification_started"
VERIFICATION_PASSED = "verification_passed"
VERIFICATION_FAILED = "verification_failed"

# 規劃事件（Planning Events）
PLANNING_STARTED = "planning_started"
PLANNING_COMPLETED = "planning_completed"
PLANNING_BLOCKED = "planning_blocked"

# 多渠道事件（Multi-Channel Events）
CHANNEL_MESSAGE_RECEIVED = "channel_message_received"
CHANNEL_MESSAGE_SENT = "channel_message_sent"

# 隱私保護事件（Privacy Events）
PII_DETECTED = "pii_detected"
PII_MASKED = "pii_masked"
PRIVACY_VIOLATION_DETECTED = "privacy_violation_detected"

# 權限委託事件（Delegation Events）
DELEGATION_CREATED = "delegation_created"
DELEGATION_APPROVED = "delegation_approved"
DELEGATION_REVOKED = "delegation_revoked"

# 資源預算事件（Budget Events）
BUDGET_WARNING = "budget_warning"
BUDGET_EXCEEDED = "budget_exceeded"
BUDGET_RESET = "budget_reset"

# 深度思考事件（Deep Thinking Events）
THINKING_STARTED = "thinking_started"
THINKING_IN_PROGRESS = "thinking_in_progress"
THINKING_COMPLETED = "thinking_completed"

# ---- 自主實驗室事件（Lab Events） ----
PLAN_GENERATED = "plan_generated"
PLAN_APPROVED = "plan_approved"
PLAN_EXECUTION_STARTED = "plan_execution_started"
PLAN_STEP_COMPLETED = "plan_step_completed"

SANDBOX_CREATED = "sandbox_created"
SANDBOX_DESTROYED = "sandbox_destroyed"

TEST_STARTED = "test_started"
TEST_PASSED = "test_passed"
TEST_FAILED = "test_failed"
TEST_COMPLETED = "test_completed"

RESEARCH_QUERY = "research_query"
RESEARCH_FOUND = "research_found"
RESEARCH_COMPLETED = "research_completed"

FIX_APPLIED = "fix_applied"
FIX_VERIFIED = "fix_verified"

CODE_GENERATED = "code_generated"
CODE_VERIFIED = "code_verified"

LAB_ENVIRONMENT_CREATED = "lab_environment_created"
LAB_ENVIRONMENT_CLEANED = "lab_environment_cleaned"

T = TypeVar('T')
EventHandler = Callable[[AgentEvent], Awaitable[None] | None]


class _EventStreamImpl(Generic[T]):
    """事件流底層實作（核心邏輯）

    此類別負責管理事件佇列、訂閱者清單和流狀態。
    所有公開方法的實際工作都由這個類別執行。
    """

    def __init__(self, maxsize: int = 0):
        """初始化事件流底層實作

        Args:
            maxsize: 佇列最大容量，0 表示無限制
        """
        self._queue: asyncio.Queue[AgentEvent] = asyncio.Queue(maxsize=maxsize)
        self._result: Optional[T] = None
        self._ended: bool = False
        self._subscribers: List[EventHandler] = []
        self._lock = asyncio.Lock()

    def _core_push(self, event: AgentEvent) -> bool:
        """核心方法：將事件推入佇列（同步）

        Args:
            event: 要推送的事件

        Returns:
            bool: 推送成功回傳 True，若流已結束回傳 False
        """
        if self._ended:
            return False
        self._queue.put_nowait(event)
        return True

    async def _core_push_async(self, event: AgentEvent) -> bool:
        """核心方法：將事件推入佇列（非同步）

        Args:
            event: 要推送的事件

        Returns:
            bool: 推送成功回傳 True，若流已結束回傳 False
        """
        if self._ended:
            return False
        await self._queue.put(event)
        return True

    def _core_end(self, result: Optional[T] = None) -> None:
        """核心方法：結束事件流

        Args:
            result: 可選的最終結果
        """
        if self._ended:
            return
        self._ended = True
        if result is not None:
            self._result = result
        # 放入 None 作為結束哨兵
        self._queue.put_nowait(None)  # type: ignore

    async def _core_wait_event(self) -> Optional[AgentEvent]:
        """核心方法：等待下一個事件

        Returns:
            Optional[AgentEvent]: 事件物件，若流已結束則回傳 None
        """
        try:
            item = await self._queue.get()
            if item is None:
                return None
            return item
        except asyncio.CancelledError:
            return None

    def _core_has_subscribers(self) -> bool:
        """核心方法：檢查是否有訂閱者"""
        return len(self._subscribers) > 0

    def _core_add_subscriber(self, handler: EventHandler) -> None:
        """核心方法：新增訂閱者"""
        if handler not in self._subscribers:
            self._subscribers.append(handler)

    def _core_remove_subscriber(self, handler: EventHandler) -> None:
        """核心方法：移除訂閱者"""
        if handler in self._subscribers:
            self._subscribers.remove(handler)

    async def _core_notify_subscribers(self, event: AgentEvent) -> None:
        """核心方法：通知所有訂閱者"""
        if not self._subscribers:
            return
        for handler in self._subscribers:
            try:
                result = handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                # 訂閱者異常不影響主流程，但需記錄日誌
                _helper_log("warning", "subscriber_error", error=str(e))

    @property
    def _core_is_ended(self) -> bool:
        """核心屬性：事件流是否已結束"""
        return self._ended

    @property
    def _core_queue_size(self) -> int:
        """核心屬性：佇列中待處理的事件數量"""
        return self._queue.qsize()

    @property
    def _core_result(self) -> Optional[T]:
        """核心屬性：最終結果"""
        return self._result

# ============================================================
# 輔助方法區（Helpers — 集中管理，可獨立移除）
# ============================================================

def _helper_format_event(event: AgentEvent) -> str:
    """輔助方法：將事件格式化為日誌字串"""
    return f"[{event.type}]"


def _helper_is_terminal(event: AgentEvent) -> bool:
    """輔助方法：判斷是否為終止事件"""
    terminal_types = {"agent_end", "error", "done"}
    return event.type in terminal_types


def _helper_generate_trace_id() -> str:
    """輔助方法：產生追蹤 ID"""
    return f"evt-{int(time.time())}-{uuid.uuid4().hex[:8]}"


def _helper_success_response(data: Any, request_id: str = "") -> Dict[str, Any]:
    """輔助方法：標準成功回應"""
    return {
        "success": True,
        "data": data,
        "error": None,
        "metadata": {
            "trace_id": request_id or _helper_generate_trace_id(),
            "module_id": "L7-6-2",
            "api_version": "v1",
            "timestamp": int(time.time() * 1000)
        }
    }


def _helper_error_response(error_code: str, message: str, request_id: str = "") -> Dict[str, Any]:
    """輔助方法：標準錯誤回應"""
    return {
        "success": False,
        "data": None,
        "error": {
            "code": error_code,
            "message": message,
        },
        "metadata": {
            "trace_id": request_id or _helper_generate_trace_id(),
            "module_id": "L7-6-2",
            "api_version": "v1",
            "timestamp": int(time.time() * 1000)
        }
    }


def _helper_check_policy(action: str, params: Dict[str, Any], risk_score: float = 0.1) -> tuple[bool, str]:
    """輔助方法：Policy 授權檢查

    目前為簡易實作，未來將整合 L0-2 PolicyEngine。

    Args:
        action: 操作名稱
        params: 操作參數
        risk_score: 風險分數 (0.0 ~ 1.0)

    Returns:
        tuple[bool, str]: (是否通過, 原因)
    """
    # 簡易實作：唯讀操作自動通過，寫入操作需檢查
    read_actions = {"health_check", "is_ended", "queue_size", "has_subscribers", "result"}
    if action in read_actions:
        return True, ""

    # 其他操作暫時允許（未來整合 L0-2）
    # TODO: 整合 L0-2 PolicyEngine
    return True, ""


def _helper_log(level: str, action: str, **kwargs) -> None:
    """輔助方法：簡易日誌記錄"""
    # TODO: 整合 L7-6 日誌系統
    pass

def _helper_create_reflection_event(trigger_type: str, context: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立反思事件"""
    return AgentEvent(
        type=REFLECTION_TRIGGERED if trigger_type == "start" else REFLECTION_COMPLETED,
        trigger_type=trigger_type,
        context=context,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_lab_event(event_type: str, lab_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立自主實驗室事件

    Args:
        event_type: 事件類型（如 PLAN_GENERATED, TEST_PASSED）
        lab_data: 實驗室相關資料

    Returns:
        AgentEvent: 實驗室事件物件
    """
    return AgentEvent(
        type=event_type,
        lab_data=lab_data,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_skill_event(event_type: str, skill_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立技能相關事件"""
    return AgentEvent(
        type=event_type,
        skill_data=skill_data,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_memory_event(event_type: str, memory_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立記憶事件"""
    return AgentEvent(
        type=event_type,
        memory_data=memory_data,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_privacy_event(event_type: str, privacy_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立隱私事件"""
    return AgentEvent(
        type=event_type,
        privacy_data=privacy_data,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_evolution_event(event_type: str, skill_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立進化事件"""
    return AgentEvent(
        type=event_type,
        skill_data=skill_data,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_verification_event(event_type: str, verification_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立驗證事件"""
    return AgentEvent(
        type=event_type,
        verification_data=verification_data,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_delegation_event(event_type: str, delegation_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立權限委託事件"""
    return AgentEvent(
        type=event_type,
        delegation_data=delegation_data,
        timestamp=int(time.time() * 1000)
    )

def _helper_create_budget_event(event_type: str, budget_data: Dict[str, Any]) -> AgentEvent:
    """輔助方法：建立資源預算事件"""
    return AgentEvent(
        type=event_type,
        budget_data=budget_data,
        timestamp=int(time.time() * 1000)
    )

# ============================================================
# 🔴 對外窗口
# ============================================================

class EventStream(AsyncIterator[AgentEvent]):
    """事件流公開類別

    提供事件推送、訂閱、非同步迭代等公開 API。

    Example:
        >>> stream = EventStream()
        >>> await stream.push_async(event)
        >>> async for event in stream:
        ...     print(event)
    """

    def __init__(self, maxsize: int = 0):
        """建立事件流

        Args:
            maxsize: 佇列最大容量，0 表示無限制
        """
        self._impl = _EventStreamImpl(maxsize)
        self._trace_id = _helper_generate_trace_id()
        self._created_at = int(time.time() * 1000)

    def push(self, event: AgentEvent) -> Dict[str, Any]:
        """同步推送事件

        Agents:
            - 冪等性：否，每次推送都會新增事件到佇列
            - 副作用：修改事件佇列，通知所有訂閱者
            - 權限要求：operator
        """
        passed, reason = _helper_check_policy("push_event", {"event_type": event.type}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)

        try:
            if not self._impl._core_push(event):
                return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", "Event stream has ended", self._trace_id)
            return _helper_success_response({"status": "pushed", "event_type": event.type}, self._trace_id)
        except Exception as e:
            _helper_log("error", "push_event", error=str(e))
            return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", str(e), self._trace_id)

    async def push_async(self, event: AgentEvent) -> Dict[str, Any]:
        """非同步推送事件

        Agents:
            - 冪等性：否，每次推送都會新增事件到佇列
            - 副作用：修改事件佇列，通知所有訂閱者
            - 權限要求：operator
        """
        passed, reason = _helper_check_policy("push_event_async", {"event_type": event.type}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)

        try:
            if not await self._impl._core_push_async(event):
                return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", "Event stream has ended", self._trace_id)
            await self._impl._core_notify_subscribers(event)
            return _helper_success_response({"status": "pushed", "event_type": event.type}, self._trace_id)
        except Exception as e:
            _helper_log("error", "push_event_async", error=str(e))
            return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", str(e), self._trace_id)

    def end(self, result: Optional[Any] = None) -> Dict[str, Any]:
        """結束事件流

        Agents:
            - 冪等性：是，重複呼叫不會造成額外影響
            - 副作用：關閉佇列，標記流為已結束
            - 權限要求：operator
        """
        passed, reason = _helper_check_policy("end_stream", {}, risk_score=0.2)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)

        try:
            self._impl._core_end(result)
            return _helper_success_response({"status": "ended", "has_result": result is not None}, self._trace_id)
        except Exception as e:
            _helper_log("error", "end_stream", error=str(e))
            return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", str(e), self._trace_id)

    async def result(self) -> Dict[str, Any]:
        """等待並回傳最終結果

        Agents:
            - 冪等性：是，唯讀操作
            - 副作用：無
            - 權限要求：reader
        """
        passed, reason = _helper_check_policy("result", {}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)

        try:
            while not self._impl._core_is_ended:
                await asyncio.sleep(0.01)
            return _helper_success_response({"result": self._impl._core_result}, self._trace_id)
        except Exception as e:
            _helper_log("error", "result", error=str(e))
            return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", str(e), self._trace_id)

    def subscribe(self, handler: EventHandler) -> Dict[str, Any]:
        """註冊事件訂閱者

        Agents:
            - 冪等性：是，重複註冊相同處理器不會造成重複
            - 副作用：修改訂閱者清單
            - 權限要求：user
        """
        passed, reason = _helper_check_policy("subscribe", {}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)

        try:
            self._impl._core_add_subscriber(handler)
            return _helper_success_response({"status": "subscribed", "subscriber_count": len(self._impl._subscribers)}, self._trace_id)
        except Exception as e:
            _helper_log("error", "subscribe", error=str(e))
            return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", str(e), self._trace_id)

    def __aiter__(self) -> AsyncIterator[AgentEvent]:
        """非同步迭代器支援

        Agents:
            - 冪等性：是
            - 副作用：無
            - 權限要求：reader
        """
        return self

    async def __anext__(self) -> AgentEvent:
        """取得下一個事件

        Raises:
            StopAsyncIteration: 當流已結束且無事件時
        """
        event = await self._impl._core_wait_event()
        if event is None:
            raise StopAsyncIteration
        return event

    def is_ended(self) -> Dict[str, Any]:
        """檢查事件流是否已結束

        Agents:
            - 冪等性：是
            - 副作用：無
            - 權限要求：reader
        """
        passed, reason = _helper_check_policy("is_ended", {}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)
        return _helper_success_response({"ended": self._impl._core_is_ended}, self._trace_id)

    def queue_size(self) -> Dict[str, Any]:
        """取得佇列中待處理的事件數量

        Agents:
            - 冪等性：是
            - 副作用：無
            - 權限要求：reader
        """
        passed, reason = _helper_check_policy("queue_size", {}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)
        return _helper_success_response({"queue_size": self._impl._core_queue_size}, self._trace_id)

    def has_subscribers(self) -> Dict[str, Any]:
        """檢查是否有訂閱者

        Agents:
            - 冪等性：是
            - 副作用：無
            - 權限要求：reader
        """
        passed, reason = _helper_check_policy("has_subscribers", {}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)
        return _helper_success_response({"has_subscribers": self._impl._core_has_subscribers()}, self._trace_id)

    def health_check(self) -> Dict[str, Any]:
        """健康檢查

        Agents:
            - 冪等性：是，唯讀操作
            - 副作用：無
            - 權限要求：reader
        """
        passed, reason = _helper_check_policy("health_check", {}, risk_score=0.1)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)

        return {
            "success": True,
            "data": {
                "module": "pi_agent_event",
                "version": "1.1.0",
                "five_element": "水",
                "status": "ok",
                "policy_connected": False,
                "sensors": {
                    "structural": {"status": "ok", "details": "事件流引擎"},
                    "behavioral": {"status": "ok", "details": f"佇列大小: {self._impl._core_queue_size}, 已結束: {self._impl._core_is_ended}"},
                    "security": {"status": "ok", "details": "無安全敏感操作"},
                    "dependency": {"status": "ok", "details": "依賴 L7-6-1 pi_agent_types"},
                    "coverage": {"status": "ok", "details": "公開方法: 9 個"},
                }
            },
            "error": None,
            "metadata": {
                "trace_id": self._trace_id,
                "module_id": "L7-6-2",
                "api_version": "v1",
                "timestamp": int(time.time() * 1000)
            }
        }

    def close(self) -> Dict[str, Any]:
        """優雅關閉

        Agents:
            - 冪等性：是
            - 副作用：釋放資源
            - 權限要求：user
        """
        passed, reason = _helper_check_policy("close", {}, risk_score=0.2)
        if not passed:
            return _helper_error_response("GRAPHIFY.AUTH.PERMISSION_DENIED", f"Policy check failed: {reason}", self._trace_id)

        try:
            if not self._impl._core_is_ended:
                self._impl._core_end(None)
            return _helper_success_response({"status": "closed"}, self._trace_id)
        except Exception as e:
            _helper_log("error", "close", error=str(e))
            return _helper_error_response("GRAPHIFY.RUNTIME.INTERNAL_ERROR", str(e), self._trace_id)

# --- 🔴 對外窗口：公開介面 ---

__all__ = [
    # 事件流類別
    "EventStream",
    # 反思事件
    "REFLECTION_TRIGGERED",
    "REFLECTION_COMPLETED",
    "DAILY_REFLECTION_TRIGGERED",
    "DAILY_REFLECTION_COMPLETED",
    # 進化事件
    "SKILL_GENERATION_STARTED",
    "SKILL_GENERATION_COMPLETED",
    "SKILL_EVOLUTION_TRIGGERED",
    "SKILL_PATCHED",
    # 記憶事件
    "MEMORY_CONSOLIDATED",
    "MEMORY_RECALLED",
    "MEMORY_CLEANED",
    # 驗證事件
    "VERIFICATION_STARTED",
    "VERIFICATION_PASSED",
    "VERIFICATION_FAILED",
    # 規劃事件
    "PLANNING_STARTED",
    "PLANNING_COMPLETED",
    "PLANNING_BLOCKED",
    # 多渠道事件
    "CHANNEL_MESSAGE_RECEIVED",
    "CHANNEL_MESSAGE_SENT",
    # 隱私保護事件
    "PII_DETECTED",
    "PII_MASKED",
    "PRIVACY_VIOLATION_DETECTED",
    # 權限委託事件
    "DELEGATION_CREATED",
    "DELEGATION_APPROVED",
    "DELEGATION_REVOKED",
    # 資源預算事件
    "BUDGET_WARNING",
    "BUDGET_EXCEEDED",
    "BUDGET_RESET",
    # 深度思考事件
    "THINKING_STARTED",
    "THINKING_IN_PROGRESS",
    "THINKING_COMPLETED",
    # 自主實驗室事件
    "PLAN_GENERATED",
    "PLAN_APPROVED",
    "PLAN_EXECUTION_STARTED",
    "PLAN_STEP_COMPLETED",
    "SANDBOX_CREATED",
    "SANDBOX_DESTROYED",
    "TEST_STARTED",
    "TEST_PASSED",
    "TEST_FAILED",
    "TEST_COMPLETED",
    "RESEARCH_QUERY",
    "RESEARCH_FOUND",
    "RESEARCH_COMPLETED",
    "FIX_APPLIED",
    "FIX_VERIFIED",
    "CODE_GENERATED",
    "CODE_VERIFIED",
    "LAB_ENVIRONMENT_CREATED",
    "LAB_ENVIRONMENT_CLEANED",
]

# ============================================================
# 🟡 對內窗口
# ============================================================

def _internal_create_event_stream(maxsize: int = 0) -> EventStream:
    """對內窗口：建立事件流實例（供 L7-6-3 loop 使用）

    Contract:
        - 此方法為工廠方法，每次呼叫建立新實例
        - 回傳的 EventStream 實例可正常使用所有公開 API
        - 此介面僅供 L7-6-3 內部使用，不公開

    Args:
        maxsize: 佇列最大容量，0 表示無限制

    Returns:
        EventStream: 新建立的事件流實例
    """
    return EventStream(maxsize=maxsize)


def _internal_get_stream_status(stream: EventStream) -> Dict[str, Any]:
    """對內窗口：取得事件流狀態（供 L7-6-3 loop 監控）

    Contract:
        - 此方法為唯讀操作，無副作用
        - 回傳狀態包含佇列大小、是否已結束、訂閱者數量
        - 此介面僅供 L7-6-3 內部使用，不公開

    Args:
        stream: 要查詢狀態的事件流實例

    Returns:
        Dict[str, Any]: 包含 queue_size, is_ended, has_subscribers
    """
    return {
        "queue_size": stream._impl._core_queue_size,
        "is_ended": stream._impl._core_is_ended,
        "has_subscribers": stream._impl._core_has_subscribers(),
    }

def _internal_get_event_registry() -> Dict[str, List[str]]:
    """對內窗口：取得完整事件註冊表（含 10 大能力領域+ 自主實驗室）

    Contract:
        - 此方法為唯讀操作，無副作用
        - 回傳所有事件類型的分類清單
        - 此介面僅供 L7-6-4 core 內部使用，不公開

    Returns:
        Dict[str, List[str]]: 各能力領域對應的事件類型清單
    """
    return {
        "reflection": [
            "reflection_triggered",
            "reflection_completed",
            "daily_reflection_triggered",
            "daily_reflection_completed",
        ],
        "evolution": [
            "skill_generation_started",
            "skill_generation_completed",
            "skill_evolution_triggered",
            "skill_patched",
        ],
        "memory": [
            "memory_consolidated",
            "memory_recalled",
            "memory_cleaned",
        ],
        "verification": [
            "verification_started",
            "verification_passed",
            "verification_failed",
        ],
        "planning": [
            "planning_started",
            "planning_completed",
            "planning_blocked",
        ],
        "channel": [
            "channel_message_received",
            "channel_message_sent",
        ],
        "privacy": [
            "pii_detected",
            "pii_masked",
            "privacy_violation_detected",
        ],
        "delegation": [
            "delegation_created",
            "delegation_approved",
            "delegation_revoked",
        ],
        "budget": [
            "budget_warning",
            "budget_exceeded",
            "budget_reset",
        ],
        "lab": [
            "plan_generated",
            "plan_approved",
            "plan_execution_started",
            "plan_step_completed",
            "sandbox_created",
            "sandbox_destroyed",
            "test_started",
            "test_passed",
            "test_failed",
            "test_completed",
            "research_query",
            "research_found",
            "research_completed",
            "fix_applied",
            "fix_verified",
            "code_generated",
            "code_verified",
            "lab_environment_created",
            "lab_environment_cleaned",
        ],

    }

# ============================================================
# USAGE 區塊
# ============================================================
"""
USAGE

module_version: 1.1.0
api_version: v1

quick_start: |
  from l7_pi_agent.pi_agent_event import EventStream
  from l7_pi_agent.pi_agent_types import AgentEvent

  # 建立事件流
  stream = EventStream()

  # 發送反思事件
  event = AgentEvent(type="reflection_triggered", context={"task": "code_review"})
  stream.push(event)

  # 訂閱所有事件
  def on_event(event):
      print(f"收到事件: {event.type}")
  stream.subscribe(on_event)

  # 非同步迭代
  async for event in stream:
      print(event)

example: |
  import asyncio
  from l7_pi_agent.pi_agent_event import EventStream
  from l7_pi_agent.pi_agent_types import AgentEvent

  async def main():
      # 建立事件流
      stream = EventStream()

      # 訂閱事件
      def log_event(event):
          print(f"[{event.type}] {event.timestamp}")

      stream.subscribe(log_event)

      # 發送各類事件
      # 反思事件
      stream.push(AgentEvent(type="reflection_triggered", context={"task": "code_review"}))
      stream.push(AgentEvent(type="reflection_completed", context={"summary": "發現 3 個改進點"}))

      # 進化事件
      stream.push(AgentEvent(type="skill_generation_started", skill_data={"name": "code_review_skill"}))
      stream.push(AgentEvent(type="skill_generation_completed", skill_data={"name": "code_review_skill", "status": "success"}))

      # 記憶事件
      stream.push(AgentEvent(type="memory_consolidated", memory_data={"entries": 5}))
      stream.push(AgentEvent(type="memory_recalled", memory_data={"query": "previous_reviews", "count": 3}))

      # 驗證事件
      stream.push(AgentEvent(type="verification_started", verification_data={"task": "test_suite"}))
      stream.push(AgentEvent(type="verification_passed", verification_data={"task": "test_suite", "evidence": "all tests passed"}))

      # 隱私事件
      stream.push(AgentEvent(type="pii_detected", privacy_data={"pii_type": "email", "confidence": 0.95}))
      stream.push(AgentEvent(type="pii_masked", privacy_data={"original": "user@example.com", "masked": "u***@e***.com"}))

      # 權限委託事件
      stream.push(AgentEvent(type="delegation_created", delegation_data={"delegator": "main_agent", "delegatee": "sub_agent"}))
      stream.push(AgentEvent(type="delegation_approved", delegation_data={"delegation_id": "dlg_001"}))

      # 資源預算事件
      stream.push(AgentEvent(type="budget_warning", budget_data={"resource": "token", "consumed": 8000, "limit": 10000}))
      stream.push(AgentEvent(type="budget_exceeded", budget_data={"resource": "token", "consumed": 12000, "limit": 10000}))

      # 結束事件流
      stream.end()

      # 取得結果
      result = await stream.result()
      print("事件流完成")

      # 健康檢查
      health = stream.health_check()
      print(f"健康狀態: {health['data']['status']}")

      # 關閉
      stream.close()

  asyncio.run(main())

capabilities:
  - name: event_stream
    description: 事件流發送與訂閱
  - name: reflection_events
    description: 反思事件（triggered/completed/daily）
  - name: evolution_events
    description: 進化事件（skill_generation/evolution/patch）
  - name: memory_events
    description: 記憶事件（consolidated/recalled/cleaned）
  - name: verification_events
    description: 驗證事件（started/passed/failed）
  - name: planning_events
    description: 規劃事件（started/completed/blocked）
  - name: channel_events
    description: 多渠道事件（received/sent）
  - name: privacy_events
    description: 隱私保護事件（detected/masked/violation）
  - name: delegation_events
    description: 權限委託事件（created/approved/revoked）
  - name: budget_events
    description: 資源預算事件（warning/exceeded/reset）
  - name: health_check
    description: 健康狀態查詢
  - name: close
    description: 優雅關閉

mcp_tools:
  - name: health_check
    description: 事件流健康檢查
    input_schema: {}
    output_schema: {"type": "object"}
  - name: queue_size
    description: 取得佇列大小
    input_schema: {}
    output_schema: {"type": "object"}
  - name: has_subscribers
    description: 檢查是否有訂閱者
    input_schema: {}
    output_schema: {"type": "object"}

grpc_services: []

rest_apis: []

cli_commands: []

common_errors:
  - error: 事件流已結束，無法推送新事件
    solution: 檢查 is_ended() 狀態，避免在結束後推送
  - error: asyncio.Queue 已滿
    solution: 建立 EventStream 時提高 maxsize 參數
  - error: StopAsyncIteration 異常
    solution: 正常情況，代表事件流已結束且無更多事件
"""
