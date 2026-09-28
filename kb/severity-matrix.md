# Incident Severity Matrix & Routing

Use this matrix to classify, prioritize, and route incidents consistently across
IT Operations (NOC) and Security Operations (SOC). It produces unified visibility
for both teams.

## Severity definitions

| Severity | Business impact            | Response target        |
|----------|----------------------------|------------------------|
| SEV-1    | Service down / data at risk| Immediate, < 15 min    |
| SEV-2    | Major degradation / revenue impact | < 1 hour        |
| SEV-3    | Minor degradation / single user | < 4 hours           |
| SEV-4    | Cosmetic / no user impact  | Next business day      |
| SEV-5    | Informing                  | Logged only            |

## Classification inputs

| Signal                  | Contribution to severity                                  |
|-------------------------|-----------------------------------------------------------|
| Alert count in window   | High counts (> 100) push to SEV-1/SEV-2                   |
| Production entity       | Production services +1 severity                           |
| Business criticality    | Customer-facing / revenue services +1 severity            |
| Exploitability (security)| Known active exploit → +1 severity                       |
| Data exposure           | PII / regulated data → force SEV-1                        |

## Routing (assignment groups)

| Category      | Assignment group | Notes                                |
|---------------|------------------|--------------------------------------|
| Database      | Database Ops     | latency, pool, replication, disk     |
| Application   | App Ops          | service latency, scale, microburst   |
| Compute       | Infra Ops        | CPU throttle, node issues            |
| Security      | SOC              | brute force, virtual patch, SIEM     |
| Network       | NetEng           | reachability, load balancer          |

## Owner resolution

- NOC owns runtime availability; SOC owns security incidents.
- When a security incident impacts availability, SOC leads and NOC co-owns
  the incident record (single incident, two assignment groups).

## Escalation

- SEV-1/SEV-2 require a named incident commander.
- Auto-close is allowed only when the verification gate confirms low residual
  risk (see `soc-outage-routing.md`).
