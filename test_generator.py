import importlib

from generator import main as generator
import _generated
from sql import query as sql


def test_generator():
    # Regenerate _generated.py from the three-states spec.
    generator("inputs/three_states.txt")
    # Reload so we pick up the freshly-written _generated.py rather than the
    # version Python cached when pytest first imported this module.
    importlib.reload(_generated)

    # Compare the output of your generated code to the output of the actual SQL query
    # Note: This only works for standard queries, not ESQL queries.
    assert _generated.query() == sql()
    