# Interrupted exchange planner

This branch preserves the unfinished request/reply planner from the explicit
2026-10-10 closeout instruction. It starts from verified development commit
`f761c77bbad2946a7f34e742c81027f62bb93c07` and is **not landing material**.

The fragment adds reply-topology types, declared/configured pure exchange-plan
APIs, correlation/completion options, and a preparation-context refactor. It was
interrupted immediately after the first implementation draft. It has not been
formatted, compiled, tested, reviewed, or exposed through the TypeScript API.
Do not assume it builds or that its semantic interpretations are correct.

The original code and design evidence remain recoverable in this commit and in
the local `design/asyncapi-development-loop-2026-10-10/` tracker. No running
exchange test, broker, Worker process, or subagent belongs to this fragment.

Resumption requires a new assignment. Review reply reference membership and
trait provenance, operation-binding treatment, partial/dynamic reply routes,
role/direction rules, limits and public API shape before integrating it. Runtime
matching, receiver readiness, ID reuse, cancellation, completion and transport
execution have not been implemented by this fragment.
