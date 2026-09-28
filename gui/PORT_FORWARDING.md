# Exposing the GUI from a local machine

When the Flask GUI runs on your laptop but the lab (e.g. Langflow, a Spark
driver, or a browser on the lab network) needs to reach it, open a **reverse
SSH tunnel** through a host in the lab. This keeps traffic inside the lab
network and avoids public-tunnel/Zscaler egress filters.

## The one-liner

```bash
ssh -o StrictHostKeyChecking=no -o GatewayPorts=yes -N -R 5001:localhost:5001 \
  mapr@ezdf-core2.ezmeral.demo.local
```

- `-R 5001:localhost:5001` — on the lab host open port `5001` and forward it to
  `localhost:5001` on your Mac (the Flask app).
- `-o GatewayPorts=yes` — bind to `0.0.0.0` on the lab host so **any** host on
  the lab network can reach the GUI, not just the SSH host itself.
- `-N` — no remote command, tunnel only; add `-f` to background it.

After it's up, the GUI is reachable from the lab at:

```
http://ezdf-core2.ezmeral.demo.local:5001
# or by IP, e.g.
http://10.1.84.208:5001
```

and from **other** lab hosts via either of those addresses (because of
`GatewayPorts=yes`).

## Requirements

1. Run it while the GUI is already listening on `localhost:5001`:
   ```bash
   cd aie-demo
   python -m demo_gui.run        # serves http://localhost:5001
   ```
2. The lab host's `sshd` must allow the remote bind to all interfaces. On the
   host, set in `/etc/ssh/sshd_config` (`root`/`sudo`):
   ```
   GatewayPorts yes
   ```
   then restart sshd: `sudo systemctl restart sshd`.
   > Note: after changing `GatewayPorts`, kill any stale `sshd` session forks
   > that still hold the old localhost-only bind, then re-connect the tunnel.

## Verify

From the lab host (or another lab box):

```bash
curl -s http://ezdf-core2.ezmeral.demo.local:5001/api/health
# => {"ok":true}
```

## Auto-reconnect (optional)

Wrap the command in `autossh` so the tunnel survives drops/reboots:

```bash
autossh -M 0 -o "ServerAliveInterval 30" -o "ServerAliveCountMax 3" \
  -o StrictHostKeyChecking=no -o GatewayPorts=yes -N \
  -R 5001:localhost:5001 mapr@ezdf-core2.ezmeral.demo.local
```

## Notes

- Pick a different remote port if `5001` is taken on the lab host, e.g.
  `-R 5050:localhost:5001` -> reachable at `http://<host>:5050`.
- The demo GUI is also a useful standalone: with no tunnel you can still open
  http://localhost:5001 in a browser on the same machine for a local-only view.
- This uses the same SSH host/credentials (`mapr@ezdf-core2…`) as the rest of
  the demo; swap in any lab host reachable from your Mac if preferred.
