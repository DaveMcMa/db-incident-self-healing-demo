# Runbook: SOC / NOC unified incident routing & close

Guidance for unified visibility across IT Operations and SOC, and for the
lifecycle/close decision.

## Unified visibility
- One incident record is owned by one team; the other co-owns (single record,
  two assignment groups).
- NOC owns runtime availability; SOC owns security incidents.
- A live Superset board shows the shared incident state across both teams.

## Routing
1. Classify the category first (Database/App/Compute/Security/Network).
2. Assign to the correct group per the severity matrix.
3. If security + availability overlap, SOC leads, NOC co-owns.

## Lifecycle & close
1. An incident is candidate for auto-close only when the verification gate
   (classifier) confirms low residual risk AND the runbook verification step
   passed.
2. The agent writes the resolution code and updates the ServiceNow-shaped
   ticket to Closed.
3. Every state change is written to the audit trail for the activity feed.

## What must NOT auto-close
- Any incident with a failed verification gate (auto-run confidence low).
- Security incidents where compromise is unconfirmed.
- Capacity/planning incidents that still require a planned change.

## Escalation
- SEV-1/SEV-2 always require a named incident commander.
- Below-threshold confidence always routes to a human.
