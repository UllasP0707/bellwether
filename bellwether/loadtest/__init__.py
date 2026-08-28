"""Load test: where this breaks, and what breaks first.

A single throughput number is a boast that moves with the machine. What is
useful is an attribution -- which phase is the ceiling, and whether it is the
algorithm, the network or the database -- so the scenarios isolate one suspect
each and are reported side by side.
"""

from bellwether.loadtest.harness import HEADERS, Result, Timing

__all__ = ["HEADERS", "Result", "Timing"]
