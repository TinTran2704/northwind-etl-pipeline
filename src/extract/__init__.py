"""
Extract phase modules. Implements Kimball Subsystems 1-3.

Subsystem 1 — Data Profiling    : src/extract/profiling.py
Subsystem 2 — Change Data Capture: src/extract/cdc/crc_cdc.py
Subsystem 3 — Extract System    : src/extract/http_csv_extractor.py
                                   src/extract/rest_json_extractor.py

Outputs immutable snapshots to data/raw/{source}/{date}/.
See docs/05-extract-phase.md for spec.
"""
