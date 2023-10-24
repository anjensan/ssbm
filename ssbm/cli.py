import argparse
import contextlib
import functools
import time

import rich
import rich.progress
import rich.table
import rich.live
import rich.panel

from ssbm import resolving
from ssbm import stats
from ssbm import work


class Duration(float):

    def __new__(cls, x=None):
        if x is None:
            return None
        else:
            return float.__new__(cls, x)

    def __rich__(self):
        t = self * 1000
        return f"{t:.2f}ms"


class Printer:

    def __init__(self, console: rich.console.Console, duration: float) -> None:

        self.console = console

        pt = self.progress_table = rich.table.Table(
            collapse_padding=True,
            show_edge=False,
            width=100,
        )
        pt.add_column("rps")
        pt.add_column("avg")
        pt.add_column("min")
        for p in [5, 10, 50, 90, 95]:
            pt.add_column(f"p{p:02}")
        pt.add_column("max")
        pt.add_column("cnt")

        self.start_time = time.time()
        self.progress = rich.progress.Progress(console=console, transient=True)
        self.progress_task = self.progress.add_task(description="combustion", total=duration)

    @contextlib.contextmanager
    def partial_result_scope(self):
        with rich.live.Live(
            rich.console.Group(
                    self.progress_table,
                    self.progress,
                ),
            console=self.console,
        ):
            yield
            self.progress.update(self.progress_task, visible=False)

    def print_partial_result(self, rr: work.RunResult):
        self.progress_table.add_row(
            str(round(rr.rps)),
            Duration(rr.stats.duration.avg),
            *[
                Duration(rr.stats.duration.get_quantile_value(p / 100))
                for p in [0, 5, 10, 50, 90, 95, 100]
            ],
            str(int(rr.stats.duration.count)),
        )
        self.progress.update(self.progress_task, completed=(rr.finish_at - self.start_time))

    def print_full_result(self, s: stats.StatsCollector):

        self.console.print()

        t = rich.table.Table()
        t.add_column("result")
        t.add_column("cnt")
        t.add_column("%")
        total = s.counts.total()
        for r, c in s.counts.most_common(20): # FIXME: pass max as cli param
            t.add_row(repr(r), str(c), format(100 * c / total, ".2f"))
        self.console.print("results:")
        self.console.print(t)
        self.console.print()

        t = rich.table.Table(width=40)
        t.add_column("metric")
        t.add_column("value")
        t.add_row("count", str(round(s.duration.count)))
        t.add_row("avg", Duration(s.duration.avg))
        t.add_row("min", Duration(s.duration.get_quantile_value(0)))
        for p in [5, 10, 50, 90, 95, 98, 99, 99.9]:
            t.add_row(f"p{p:02}", Duration(s.duration.get_quantile_value(p / 100)))
        t.add_row("max", Duration(s.duration.get_quantile_value(1)))
        self.console.print("stats:")
        self.console.print(t)


    def print_result(self, rr: work.RunResult):
        if rr.completed:
            self.print_full_result(rr)
        else:
            self.print_partial_result(rr)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ssbm cli runner")
    parser.add_argument("-w", "--workers", help="Number of processes", type=int, default=1)
    parser.add_argument("-t", "--threads", help="Number of threads per worker process", type=int, default=1)
    parser.add_argument("-d", "--duration", help="Duration in seconds", type=int, default=float('inf'))
    parser.add_argument("func", type=str, help="Python callable name")
    parser.add_argument("param", metavar="KEY=VALUE", nargs="*", help="Function named argument")
    return parser


def main(args=None):

    console = rich.console.Console()

    from rich.traceback import install
    install(show_locals=True)

    parser = create_parser()
    opts = parser.parse_args(args)

    func_name = opts.func
    func = resolving.resolve_obj(func_name)

    params = {}
    for p in opts.param:
        k, v = p.split("=", 1)
        if k in params:
            raise ValueError("Duplicated parameter", k)
        try:
            v = eval(v, {}, {})
        except Exception:
            pass
        params[k] = v

    console.print("combustionman")
    console.log("call function", repr(func_name))
    if params:
        console.log("with parameters", params)

    console.log("use", opts.workers, "workers")
    console.log("each has", opts.threads, "threads")
    console.log("run for", opts.duration, "seconds")

    console.log("running...")

    duration = opts.duration

    try:
        res = work.run_func(
            func=functools.partial(func, **params) if params else func,
            duration=duration,
            processes=opts.workers,
            threads=opts.threads,
            log=console.log,
            flush_each=1,
        )

        p = Printer(console, duration=duration)
        with p.partial_result_scope():
            total = stats.StatsCollector()
            for r in res:
                total.merge(r.stats)
                p.print_partial_result(r)

        p.print_full_result(total)

    except KeyboardInterrupt:
        console.log("interrupted!", style='red')
    else:
        console.log("done", style='blue')

