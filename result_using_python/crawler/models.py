from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RowTask:
    row_number: int
    car_info: str
    brand: str


@dataclass
class CandidateResult:
    fuel_type: str
    url: str
    url_prove: bool
    score: int


def unknown_result() -> CandidateResult:
    return CandidateResult(fuel_type="unknown", url="", url_prove=False, score=0)
