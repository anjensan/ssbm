from __future__ import annotations

import abc
import multiprocessing
import multiprocessing.connection
import queue
import random
import select
import threading
import time
import traceback

from dataclasses import dataclass
from typing import Any, Callable, Iterable

import ssbm.stats as stats


NoArgCallback = Callable[[], Any]


@dataclass
class RunResult:

    finish_at: float
    stats: stats.StatsCollector

    @property
    def duration(self):
        return self.finish_at - self.stats.start_at

    @property
    def rps(self):
        return self.stats.duration.count / self.duration


# TODO: implement worker which can utilize gevent
# There are known promlems mixing gevent with multiprocessing... so more investigation is needed

class Worker:

    flush_each_secs: float = 0.5

    @abc.abstractmethod
    def _start_thread(self, main: NoArgCallback):
        raise NotImplementedError

    def __init__(
            self,
            threads: int,
            inq: multiprocessing.connection.Connection,
            outq: multiprocessing.connection.Connection,

    ) -> None:
        self._threads = threads
        self._inq = inq
        self._outq = outq
        self._func_event = threading.Event()
        self._run_res_queue = queue.SimpleQueue()
        self._done = False
        self.stats = stats.StatsCollector()

    def _flush_stats(self):
        self._outq.send(self.stats)
        self.stats.clear()

    def _add_stats(self, runs: list[stats.RunInfo]):
        self.stats.update(runs)

    def _collect_stats_loop(self):
        next_flush_at = time.time()
        while True:
            runs = self._run_res_queue.get()
            self._add_stats(runs)
            now = time.time()
            if now > next_flush_at:
                next_flush_at = random.uniform(self.flush_each_secs / 2, self.flush_each_secs) + now
                self._flush_stats()

    def _handle_messages_loop(self):
        while not self._done:
            try:
                m, args = self._inq.recv()
            except KeyboardInterrupt:
                self.shutdown()
                return
            m(self, *args)

    def start(self):
        for i in range(self._threads):
            self._start_thread(self._worker_main_loop)

        t = threading.Thread(target=self._collect_stats_loop, daemon=True)
        t.start()

        self._handle_messages_loop()

    def _worker_main_loop(self):
        while not self._done:

            self._func_event.wait()
            f = self._func
            if f is None:
                continue

            try:
                s = stats.make_calls(f, self._chunk_size)
                self._run_res_queue.put(s)
            except Exception:
                traceback.print_exc()  # FIXME: better logging
                raise

    def set_func(self, func: NoArgCallback, chunk: int = 100):
        self._func = func
        self._chunk_size = chunk
        self._func_event.set()

    def unset_func(self):
        self._func_event.clear()
        self._func = None
        self.stats.clear()

    def shutdown(self):
        self._done = True
        self._func_event.set()

    def _start_thread(self, target: NoArgCallback):
        t = threading.Thread(target=target, daemon=True)
        t.start()


class Runner:

    _min_listen_timeout = 0.5

    def __init__(
            self,
            processes: int,
            threads: int,
            log: Callable,
    ) -> None:
        self._threads = threads
        self._workers = processes
        self._outs: list[multiprocessing.connection.Connection] = []
        self._ins: list[multiprocessing.connection.Connection] = []
        self._processes: list[multiprocessing.Process] = []
        self.stats: stats.StatsCollector = stats.StatsCollector()
        self.log = log or (lambda *_: None)

    def _spawn_process(self) -> multiprocessing.Process:
        a, b = multiprocessing.Pipe()
        w = Worker(
            threads=self._threads,
            inq=a,
            outq=a,
        )
        p = multiprocessing.Process(target=w.start, args=())
        self._processes.append(p)
        self._ins.append(b)
        self._outs.append(b)
        p.start()
        self.log("spawned process", p.pid)

    def _listen(
            self,
            duration: float,
            flush_each: float,
            out: queue.SimpleQueue,
        ):

        now = time.time()
        stop_at = time.time() + duration
        flush_at = now + flush_each

        while (r := stop_at - time.time()) > 0:
            now = time.time()

            if now > flush_at:
                out.put((now, self.stats.copy()))
                self.stats.clear()
                flush_at += flush_each

            xs, _, _, = select.select(self._ins, [], [], min(stop_at - now, flush_at - now, 1))
            for x in xs:
                x: multiprocessing.connection.Connection
                try:
                    m = x.recv()  # this still may block
                except (ConnectionResetError, EOFError):
                    break  # FIXME
                self._handle_message(m)

        out.put(None)

    def _handle_message(self, m):
        self.stats.merge(m)

    def _send_to_all(self, m: Callable, *args):
        for x in self._outs:
            x.send((m, args))

    def start(self):
        for i in range(self._workers):
            self._spawn_process()
        self.log("spawned", self._workers, "processes")

    def run(
            self,
            func: NoArgCallback,
            duration: float,
            flush_each: float = 0.1,
    ) -> Iterable[RunResult]:

        self._send_to_all(Worker.set_func, func)

        q = queue.SimpleQueue()
        t = threading.Thread(target=lambda: self._listen(duration, flush_each, q), daemon=True)
        t.start()

        while s := q.get():
            at, ss = s
            yield RunResult(finish_at=at, stats=ss)
        self._send_to_all(Worker.unset_func)

        t.join()
        yield RunResult(finish_at=time.time(), stats=self.stats.copy())
        self.stats.clear()

    def shutdown(self, timeout=1):
        self._send_to_all(Worker.shutdown)

        wait_until = time.time() + timeout
        for p in self._processes:
            self.log("stop worker", p.pid)
            p.join(wait_until - time.time())

        for p in self._processes:
            if p.is_alive():
                self.log("kill worker", p.pid)
                p.kill()
        for p in self._processes:
            p.join()


def run_func(
    func: NoArgCallback,
    duration: float,
    processes: int = 1,
    threads: int = 1,
    flush_each: float = 1,
    log: Callable | None = None,
) -> Iterable[RunResult]:

    runner = Runner(
        processes=processes,
        threads=threads,
        log=log,
    )

    try:
        runner.start()
        yield from runner.run(func, duration, flush_each)
    finally:
        runner.shutdown()
