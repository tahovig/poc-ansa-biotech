from scoring import score_sequence


def test_balanced_short_sequence_is_feasible():
    result = score_sequence("ACGTTGCATGACGTACGCTA")
    assert result["feasible"] is True
    assert result["score"] > 0.7
    assert result["reasons"] == []


def test_extremely_gc_rich_sequence_is_flagged():
    result = score_sequence("GCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGCGC")
    assert result["flags"]["gc_content"] > 0.9
    assert "high gc content" in " ".join(result["reasons"]).lower()
    assert result["feasible"] is False


def test_long_homopolymer_run_is_flagged():
    result = score_sequence("ACGT" + "A" * 15 + "ACGT")
    assert result["flags"]["max_homopolymer_run"] == 16
    assert "homopolymer" in " ".join(result["reasons"]).lower()
    assert result["feasible"] is False


def test_repeat_region_is_detected():
    result = score_sequence("ACGTACGT" + "CAGCAG" * 6 + "ACGTACGT")
    assert len(result["flags"]["repeat_regions"]) >= 1
    assert "repeat" in " ".join(result["reasons"]).lower()
    assert result["feasible"] is False


def test_empty_sequence_raises():
    import pytest
    with pytest.raises(ValueError):
        score_sequence("")
