"""Claude integration: column classification and fix suggestions.

Guardrails:
- Only column profiles and masked samples are sent (see ``masking``).
- Raw values are sent only for non-personal types (amounts, flags, counts),
  unless ``send_raw_values`` is explicitly enabled (synthetic demo data only).
- Output must parse into a Pydantic schema; one retry, then give up gracefully.
- LLM suggestions are capped at ``medium`` confidence, so they are never
  auto-applied. A human always confirms them.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, TypeVar

from pydantic import BaseModel, Field, ValidationError

from smartingest.masking import UNMASKED_TYPES, mask_for_type
from smartingest.models import ColumnProfile, Confidence, SemanticType

logger = logging.getLogger(__name__)

DEFAULT_FAST_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_SMART_MODEL = "claude-sonnet-5-5"
MAX_ITEMS_PER_CALL = 50
MAX_VALUE_CHARS = 200  # cell text is untrusted input; keep what reaches the prompt short

T = TypeVar("T", bound=BaseModel)


# --- Schemas the model must answer in --------------------------------------------


class ColumnTypeAnswer(BaseModel):
    semantic_type: SemanticType
    confidence: float = Field(ge=0, le=1)
    reason: str


class FixItem(BaseModel):
    row_index: int
    suggestion: str | None
    confidence: Confidence
    reason: str


class FixAnswer(BaseModel):
    fixes: list[FixItem]


@dataclass(frozen=True)
class LLMCallRecord:
    purpose: str
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: int
    ok: bool


class Advisor(Protocol):
    def classify_column(self, profile: ColumnProfile, samples: list[str]) -> ColumnTypeAnswer | None: ...

    def suggest_fixes(
        self, profile: ColumnProfile, items: list[tuple[int, str, str]]
    ) -> list[FixItem]: ...


class NullAdvisor:
    """Used when no API key is configured: the pipeline runs rule-only."""

    def classify_column(self, profile: ColumnProfile, samples: list[str]) -> ColumnTypeAnswer | None:
        return None

    def suggest_fixes(self, profile: ColumnProfile, items: list[tuple[int, str, str]]) -> list[FixItem]:
        return []


CLASSIFY_SYSTEM = (
    "You classify spreadsheet columns for a data-quality pipeline. "
    "Allowed semantic_type values: " + ", ".join(t.value for t in SemanticType) + ". "
    "Personal values are masked: letters become 'a'/'A', digits become '9'; public email domains are kept. "
    'Answer with JSON only: {"semantic_type": ..., "confidence": 0..1, "reason": "<one short sentence>"}.'
)

FIX_SYSTEM = (
    "You repair invalid cell values for a data-quality pipeline. Canonical forms: "
    "email=lowercase local@domain.tld; phone_id=+628XXXXXXXX; date=YYYY-MM-DD; "
    "currency_idr and integer=plain digits without separators; decimal=digits with '.' decimal point; "
    "boolean=true/false; text=trimmed single spaces. "
    "Never invent information that cannot be inferred from the value and column context. "
    "If unsure, return suggestion null and confidence 'none'. "
    'Answer with JSON only: {"fixes": [{"row_index": int, "suggestion": str|null, '
    '"confidence": "high"|"medium"|"low"|"none", "reason": "<short, in Indonesian>"}]}.'
)


def _extract_json(text: str) -> dict:
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("no JSON object in response")
    return json.loads(text[start : end + 1])


class ClaudeAdvisor:
    def __init__(
        self,
        api_key: str | None = None,
        fast_model: str = DEFAULT_FAST_MODEL,
        smart_model: str = DEFAULT_SMART_MODEL,
        send_raw_values: bool = False,
        on_call: Callable[[LLMCallRecord], None] | None = None,
        max_items: int | None = None,
    ) -> None:
        import anthropic  # imported lazily so rule-only mode needs no SDK

        self._client = anthropic.Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"))
        self.fast_model = fast_model
        self.smart_model = smart_model
        self.send_raw_values = send_raw_values
        self._on_call = on_call or (lambda record: None)
        self._items_left = max_items  # cost cap per upload; None = unlimited

    def _call_json(self, purpose: str, model: str, system: str, payload: dict, schema: type[T]) -> T | None:
        user = json.dumps(payload, ensure_ascii=False)
        for attempt in (1, 2):
            started = time.perf_counter()
            ok = False
            usage_in = usage_out = 0
            try:
                response = self._client.messages.create(
                    model=model,
                    max_tokens=2048,
                    system=system,
                    messages=[{"role": "user", "content": user}],
                )
                usage_in, usage_out = response.usage.input_tokens, response.usage.output_tokens
                text = "".join(block.text for block in response.content if block.type == "text")
                result = schema.model_validate(_extract_json(text))
                ok = True
                return result
            except (ValueError, ValidationError) as exc:
                logger.warning("LLM %s returned unusable output (attempt %d): %s", purpose, attempt, exc)
            except Exception:  # network / API errors: degrade to rule-only
                logger.exception("LLM %s call failed", purpose)
                return None
            finally:
                self._on_call(
                    LLMCallRecord(purpose, model, usage_in, usage_out, int((time.perf_counter() - started) * 1000), ok)
                )
        return None

    def classify_column(self, profile: ColumnProfile, samples: list[str]) -> ColumnTypeAnswer | None:
        payload = {
            "column_name": profile.original_name,
            "null_ratio": profile.null_ratio,
            "distinct_count": profile.distinct_count,
            "rule_based_guess": profile.semantic_type.value,
            "masked_samples": [mask_for_type(s, profile.semantic_type) for s in samples[:8]],
        }
        return self._call_json("classify_column", self.fast_model, CLASSIFY_SYSTEM, payload, ColumnTypeAnswer)

    def suggest_fixes(self, profile: ColumnProfile, items: list[tuple[int, str, str]]) -> list[FixItem]:
        """items = (row_index, original_value, rule). Returns at most MEDIUM-confidence fixes."""
        raw_allowed = self.send_raw_values or profile.semantic_type in UNMASKED_TYPES
        if not raw_allowed:
            # A suggestion built from a masked value would be useless; skip value repair.
            return []
        if self._items_left is not None:
            items = items[: max(self._items_left, 0)]
            self._items_left -= len(items)
        fixes: list[FixItem] = []
        for start in range(0, len(items), MAX_ITEMS_PER_CALL):
            batch = items[start : start + MAX_ITEMS_PER_CALL]
            payload = {
                "column_name": profile.original_name,
                "semantic_type": profile.semantic_type.value,
                "examples_of_valid_values": list(profile.top_values)[:3],
                "invalid_values": [
                    {"row_index": r, "value": v[:MAX_VALUE_CHARS], "failed_rule": rule} for r, v, rule in batch
                ],
            }
            answer = self._call_json("suggest_fixes", self.smart_model, FIX_SYSTEM, payload, FixAnswer)
            if answer is None:
                continue
            allowed_rows = {r for r, _, _ in batch}
            for fix in answer.fixes:
                if fix.row_index not in allowed_rows or fix.suggestion is None:
                    continue
                if fix.confidence is Confidence.HIGH:
                    fix = fix.model_copy(update={"confidence": Confidence.MEDIUM})
                fixes.append(fix)
        return fixes


def build_advisor(
    on_call: Callable[[LLMCallRecord], None] | None = None,
    max_items: int | None = None,
    allow_raw_values: bool = True,
) -> Advisor:
    """ClaudeAdvisor when ANTHROPIC_API_KEY is set and the SDK is installed, else NullAdvisor.

    ``allow_raw_values=False`` forces masking regardless of SMARTINGEST_SEND_RAW_VALUES;
    the API passes False for public demo visitors, whose files may hold real personal data.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return NullAdvisor()
    try:
        return ClaudeAdvisor(
            fast_model=os.environ.get("SMARTINGEST_FAST_MODEL", DEFAULT_FAST_MODEL),
            smart_model=os.environ.get("SMARTINGEST_SMART_MODEL", DEFAULT_SMART_MODEL),
            send_raw_values=allow_raw_values
            and os.environ.get("SMARTINGEST_SEND_RAW_VALUES", "false").lower() == "true",
            on_call=on_call,
            max_items=max_items,
        )
    except ImportError:
        logger.warning("anthropic SDK not installed; running rule-only")
        return NullAdvisor()
