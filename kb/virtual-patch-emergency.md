# Runbook: Virtual patching / urgent vulnerability self-heal

Applies when an urgent vulnerability cannot be remediated immediately.

## Trigger
- A CVE is published with active exploit, but the patch cannot be applied now
  (reboot required, change window, third-party dependency).
- Source events from SIEM or vulnerability scan mark the asset as exposed.

## Strategy
Virtual patching contains the exploit without installing the vendor fix.

## Fix steps (virtual patch)
1. Identify the affected asset (e.g. `db-prod-01` service, public endpoint).
2. Apply an inline WAF/firewall rule that blocks the exploit signature.
3. If network-layer: deny traffic to the vulnerable endpoint from untrusted
   sources.
4. If app-layer: enable the WAF virtual-patch rule for the CVE signature.
5. Schedule the real patch in the next change window; track to completion.

## Verification
- Exploit traffic is blocked; asset passes the CVE check via WAF.
- No availability impact to legitimate traffic.

## Auto-run assessment
Virtual patch rules are safe to auto-run when they only block the exploit
signature. Rules that could drop legitimate traffic require human verify.
