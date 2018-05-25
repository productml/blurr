import pytest

from blurr.core.evaluation import ScopedEvaluationContext, EvaluationContext, Record


def test_record_clears_from_context_after_execution():
    record = Record({"id": "12345"})
    evaluation_context = EvaluationContext()
    with ScopedEvaluationContext(evaluation_context, record) as scoped_evaluation_context:
        pass

    assert "source" not in evaluation_context.global_context


def test_record_exists_inside_execution_block():
    record = Record({"id": "12345"})
    evaluation_context = EvaluationContext()
    with ScopedEvaluationContext(evaluation_context,
                                 {"source": record}) as scoped_evaluation_context:
        assert "source" in scoped_evaluation_context.global_context
        assert scoped_evaluation_context.global_context["source"] == record


def test_record_clears_from_context_when_error_in_execution_block():
    record = Record({"id": "12345"})
    evaluation_context = EvaluationContext()
    try:
        with ScopedEvaluationContext(evaluation_context, record) as scoped_evaluation_context:
            raise Exception()
    except Exception:
        assert "source" not in evaluation_context.global_context
