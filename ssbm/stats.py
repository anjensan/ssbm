from __future__ import annotations

import collections
import time
import typing

from typing import Any, Iterable

import ddsketch


class Fail(typing.NamedTuple):

    type: str
    args: tuple[Any, ...]

    @classmethod
    def from_ex(self, e: BaseException) -> Fail:
        return Fail(type(e).__qualname__, e.args)


class RunInfo(typing.NamedTuple):
    at_unix: float
    duation: float
    result: Any


class StatsCollector:

    def __init__(self) -> None:
        self.duration = ddsketch.DDSketch()
        self.counts = collections.Counter()
        self.start_at = time.time()

    def update(self, runs: Iterable[RunInfo]):
        for r in runs:
            self.duration.add(r.duation)
            self.counts[r.result] += 1

    def merge(self, other: StatsCollector):
        self.duration.merge(other.duration)
        self.counts.update(other.counts)
        self.start_at = min(self.start_at, other.start_at)

    def clear(self):
        self.duration = ddsketch.DDSketch()
        self.counts = collections.Counter()
        self.start_at = time.time()

    def copy(self) -> StatsCollector:
        res = StatsCollector()
        res.merge(self)
        return res


def make_calls(
    callback: typing.Callable[[], Any],
    times: int,
) -> list[RunInfo]:

    ret: list[RunInfo] = []

    for _ in range(times):
        start = time.perf_counter()
        result = None
        try:
            result = callback()
        except Exception as e:
            result = Fail.from_ex(e)
        finally:
            duration = time.perf_counter() - start
            ret.append(RunInfo(start, duration, result))

    return ret
