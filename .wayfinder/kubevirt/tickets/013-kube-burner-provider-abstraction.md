# kube-burner Provider Abstraction

**Label:** `wayfinder:grilling`  
**Status:** Open  
**Blocked by:** [Tier 1 Live Test](006-tier1-live-test.md)

## Question

Design the provider abstraction for VM creation backend: `provider: native` (current batched `oc apply`) vs `provider: kube_burner` (kube-burner init with VM object templates + replicas).

Decisions:
1. **Interface boundary** — what's the contract between the OADP orchestrator and the provider? Create VMs, return names? Or create + wait + report?
2. **kube-burner integration** — invoke as subprocess? Shell out to `kube-burner init --config <generated-config>`? Where does the config come from?
3. **When to use which** — is kube-burner always better at scale, or is native preferred for small counts (< 10 VMs) due to simpler debugging?
4. **kube-burner binary** — must be in the benchmark-runner container image. How to add it? Build step? Download at runtime?
5. **Metrics** — kube-burner emits its own metrics (vmiLatency, pvcLatency). Should these be captured in oadp-report.json?
