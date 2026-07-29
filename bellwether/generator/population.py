"""Synthetic employee population.

Deterministic for a given seed, so a demo tells the same story twice and a test
can assert on a specific employee.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from bellwether.events.schema import Employee
from bellwether.generator.personas import PERSONAS, PERSONAS_BY_NAME, Persona

# (department, share, p(handles financial data), p(has admin access))
_DEPARTMENTS: tuple[tuple[str, float, float, float], ...] = (
    ("engineering", 0.30, 0.02, 0.45),
    ("sales", 0.20, 0.10, 0.02),
    ("customer_success", 0.12, 0.05, 0.03),
    ("marketing", 0.10, 0.03, 0.02),
    ("finance", 0.08, 0.95, 0.05),
    ("people_ops", 0.07, 0.30, 0.08),
    ("legal", 0.05, 0.20, 0.02),
    ("it_security", 0.05, 0.02, 0.90),
    ("executive", 0.03, 0.60, 0.20),
)

_SENIORITY: tuple[tuple[str, float], ...] = (
    ("junior", 0.28),
    ("mid", 0.40),
    ("senior", 0.22),
    ("staff", 0.07),
    ("director", 0.03),
)

_LOCATIONS: tuple[tuple[str, float], ...] = (
    ("San Francisco", 0.35),
    ("New York", 0.20),
    ("Remote US", 0.25),
    ("London", 0.10),
    ("Toronto", 0.10),
)

_FIRST_NAMES = (
    "Dana", "Priya", "Alex", "Ravi", "Mei", "Jordan", "Sam", "Aisha",
    "Nikhil", "Elena", "Tomas", "Kwame", "Yuki", "Rosa", "Omar", "Hana",
    "Liam", "Zara", "Diego", "Ingrid", "Kai", "Noor", "Felix", "Amara",
)  # fmt: skip
_LAST_NAMES = (
    "Okafor", "Nakamura", "Silva", "Patel", "Chen", "Novak", "Haddad",
    "Lindqvist", "Osei", "Ferreira", "Kowalski", "Reyes", "Bhatt", "Aliyev",
    "Moreau", "Sandoval", "Iqbal", "Vasquez", "Lin", "Grant",
)  # fmt: skip


@dataclass(frozen=True, slots=True)
class PopulatedEmployee:
    """An employee plus the persona driving their simulated behavior.

    The persona is generator-only. It never reaches an event or the scorer —
    the platform has to infer risk from behavior, which is the whole point.
    """

    employee: Employee
    persona: Persona


def _weighted(rng: random.Random, options: tuple[tuple[str, float], ...]) -> str:
    return rng.choices([o[0] for o in options], weights=[o[1] for o in options])[0]


def _assign_persona(
    rng: random.Random,
    employee: Employee,
    tenure_days: int,
) -> Persona:
    """Pick a persona, letting the employee's dimensions override the draw.

    Two overrides, both because the correlation exists in reality and a demo
    that lacks it looks synthetic: people under ~90 days behave like new hires
    regardless of disposition, and attackers target executives and finance
    specifically.
    """
    if tenure_days < 90 and rng.random() < 0.75:
        return PERSONAS_BY_NAME["onboarding"]

    if (employee.is_executive or employee.handles_financial_data) and rng.random() < 0.45:
        return PERSONAS_BY_NAME["targeted"]

    return rng.choices(list(PERSONAS), weights=[p.share for p in PERSONAS])[0]


def build_population(
    size: int = 500,
    tenant_id: str = "acme",
    seed: int = 1337,
) -> list[PopulatedEmployee]:
    """Build a population of `size` employees.

    Args:
        size: Number of employees.
        tenant_id: Tenant these employees belong to.
        seed: Fixes the whole population, including which employee is E0042.

    Returns:
        Employees with ids `E0000`..`E{size-1}`, in id order.
    """
    rng = random.Random(seed)
    population: list[PopulatedEmployee] = []

    for i in range(size):
        employee_id = f"E{i:04d}"
        department, _, p_finance, p_admin = rng.choices(
            _DEPARTMENTS, weights=[d[1] for d in _DEPARTMENTS]
        )[0]
        seniority = _weighted(rng, _SENIORITY)

        # Long-tailed tenure: most people are recent, a few are very tenured.
        tenure_days = int(rng.betavariate(1.6, 3.0) * 2200) + 5

        first = rng.choice(_FIRST_NAMES)
        last = rng.choice(_LAST_NAMES)

        employee = Employee(
            employee_id=employee_id,
            tenant_id=tenant_id,
            department=department,
            seniority=seniority,
            tenure_days=tenure_days,
            location=_weighted(rng, _LOCATIONS),
            has_admin_access=rng.random() < p_admin,
            handles_financial_data=rng.random() < p_finance,
            is_executive=department == "executive" or seniority == "director",
            email=f"{first.lower()}.{last.lower()}@{tenant_id}.example",
            display_name=f"{first} {last}",
        )

        population.append(
            PopulatedEmployee(
                employee=employee,
                persona=_assign_persona(rng, employee, tenure_days),
            )
        )

    # Managers are assigned in a second pass so every manager id resolves to a
    # real employee. A dangling manager reference breaks escalation at exactly
    # the moment escalation matters.
    seniors = [p.employee.employee_id for p in population if p.employee.is_executive]
    if seniors:
        resolved: list[PopulatedEmployee] = []
        for p in population:
            if p.employee.is_executive:
                resolved.append(p)
                continue
            manager = rng.choice(seniors)
            resolved.append(
                PopulatedEmployee(
                    employee=p.employee.model_copy(update={"manager_id": manager}),
                    persona=p.persona,
                )
            )
        population = resolved

    return population
