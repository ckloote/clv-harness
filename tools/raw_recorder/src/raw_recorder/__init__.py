"""R0 raw evidence recorder (DESIGN.md §13 R0).

Writes the §7.1 archive format, as amended by
docs/decisions/2026-10-06-r0-recorder.md. Captures; never parses, repairs or
trades.
"""

RECORDER_VERSION = "0.1.0"
# Redaction rules applied before anything is written (see redact.py).
REDACTION_POLICY = "r0-v1"
