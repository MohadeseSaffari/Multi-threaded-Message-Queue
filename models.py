# models.py
from __future__ import annotations
from dataclasses import dataclass, field
import time

@dataclass(frozen=True)
class Message:
    id: int
    content: str
    created_at: float = field(default_factory=time.time)
    priority: int = 1     # 1=low, 2=med, 3=high
    work_ms: int = 50     # simulated processing time (ms)