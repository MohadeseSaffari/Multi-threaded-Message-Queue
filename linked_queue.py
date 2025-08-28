# linked_queue.py
from __future__ import annotations
from typing import Optional, Callable, Iterator
from models import Message

class _Node:
    __slots__ = ("value", "next")
    def __init__(self, value: Message) -> None:
        self.value: Message = value
        self.next: Optional[_Node] = None

class LinkedQueue:
    __slots__ = ("_head", "_tail", "_size")
    def __init__(self) -> None:
        self._head: Optional[_Node] = None
        self._tail: Optional[_Node] = None
        self._size: int = 0

    def __len__(self) -> int: return self._size
    def is_empty(self) -> bool: return self._size == 0

    def enqueue(self, msg: Message) -> None:
        node = _Node(msg)
        if self._tail is None:
            self._head = self._tail = node
        else:
            self._tail.next = node
            self._tail = node
        self._size += 1

    def dequeue(self) -> Message:
        if self._head is None:
            raise IndexError("dequeue from empty queue")
        node = self._head
        self._head = node.next
        if self._head is None:
            self._tail = None
        self._size -= 1
        return node.value

    def peek(self) -> Message:
        if self._head is None:
            raise IndexError("peek from empty queue")
        return self._head.value

    def __iter__(self) -> Iterator[Message]:
        cur = self._head
        while cur:
            yield cur.value
            cur = cur.next

    def remove_if(self, pred: Callable[[Message], bool]) -> int:
        """Remove nodes where pred(msg) is True. Returns count removed."""
        removed = 0
        while self._head and pred(self._head.value):
            self._head = self._head.next
            self._size -= 1
            removed += 1
        if self._head is None:
            self._tail = None
            return removed
        prev = self._head
        cur = prev.next
        while cur:
            if pred(cur.value):
                prev.next = cur.next
                if cur is self._tail:
                    self._tail = prev
                self._size -= 1
                removed += 1
                cur = prev.next
            else:
                prev, cur = cur, cur.next
        return removed