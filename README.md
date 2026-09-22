# blackbox — fleet telemetry backend

A resilient, reproducible telemetry service for a simulated inspection fleet
(drone, ground rover, quadruped). No AI/ML inside the product — this is the
systems layer: ingest → queue → worker → persistence → query, built to fail
predictably instead of quietly.

Status: repository scaffolding only. Architecture, data contract, and failure
policy are being written up in [`docs/PLAN.md`](docs/PLAN.md) before any
implementation code lands — see that file for the real design.

Full setup/run/demo/test instructions will be added here once the system is
buildable end to end.
