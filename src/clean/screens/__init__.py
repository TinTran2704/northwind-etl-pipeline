"""
Data quality screens for the Clean phase. Implements Kimball Subsystem #4.

Screen hierarchy (docs/06-clean-phase.md §6.3):
  1. ColumnPropertyScreen — per-column constraints
  2. StructureScreen      — referential integrity across tables
  3. DataRuleScreen       — cross-column business rules
  4. ReasonabilityScreen  — statistical drift detection
"""
