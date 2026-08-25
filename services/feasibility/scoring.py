import re

GC_RICH_THRESHOLD = 0.65
MAX_HOMOPOLYMER_THRESHOLD = 10
MIN_REPEAT_UNIT = 2
MAX_REPEAT_UNIT = 6
MIN_REPEAT_COUNT = 4


def _gc_content(sequence: str) -> float:
    gc = sum(1 for base in sequence if base in "GC")
    return gc / len(sequence)


def _max_homopolymer_run(sequence: str) -> int:
    longest = 1
    current = 1
    for i in range(1, len(sequence)):
        if sequence[i] == sequence[i - 1]:
            current += 1
            longest = max(longest, current)
        else:
            current = 1
    return longest if sequence else 0


def _repeat_regions(sequence: str) -> list[dict]:
    regions = []
    for unit_len in range(MIN_REPEAT_UNIT, MAX_REPEAT_UNIT + 1):
        pattern = re.compile(r"(.{%d})\1{%d,}" % (unit_len, MIN_REPEAT_COUNT - 1))
        for match in pattern.finditer(sequence):
            regions.append({
                "start": match.start(),
                "end": match.end(),
                "unit": match.group(1),
            })
    return regions


def score_sequence(sequence: str) -> dict:
    if not sequence:
        raise ValueError("sequence must not be empty")

    gc_content = _gc_content(sequence)
    max_run = _max_homopolymer_run(sequence)
    repeats = _repeat_regions(sequence)

    reasons = []
    penalty = 0.0

    if gc_content > GC_RICH_THRESHOLD:
        reasons.append(f"High GC content ({gc_content:.0%}) increases synthesis difficulty")
        penalty += (gc_content - GC_RICH_THRESHOLD) * 2

    if max_run > MAX_HOMOPOLYMER_THRESHOLD:
        reasons.append(f"Homopolymer run of {max_run} bases exceeds safe threshold")
        penalty += 0.3

    if repeats:
        reasons.append(f"{len(repeats)} repeat region(s) detected")
        penalty += 0.1 * len(repeats)

    score = max(0.0, min(1.0, 1.0 - penalty))
    feasible = len(reasons) == 0

    return {
        "score": round(score, 2),
        "feasible": feasible,
        "reasons": reasons,
        "flags": {
            "gc_content": round(gc_content, 2),
            "max_homopolymer_run": max_run,
            "repeat_regions": repeats,
        },
    }
