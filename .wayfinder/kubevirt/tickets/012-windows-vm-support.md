# Windows VM Support

**Label:** `wayfinder:grilling`  
**Status:** Open  
**Blocked by:** [Golden Image Lifecycle](002-golden-image-lifecycle.md), [Tier 1 Live Test](006-tier1-live-test.md)

## Question

Design the Windows VM support layer. The golden image (qcow2 with Windows Server + MSSQL + HammerDB + FIO pre-installed) auto-starts HammerDB on boot via a scheduled task. Key decisions:

1. **VM template variant** — Windows VMs need different firmware (EFI), clock config (Hyper-V enlightenments), TPM, SMM. Separate template or conditional blocks in existing `vm_template.yaml`?
2. **SSH access** — Windows uses `accessCredentials` with `qemuGuestAgent` SSH key propagation (not cloud-init). How does the profile express this?
3. **Disk initialization** — Windows data disks need GPT + NTFS formatting. Is this handled by the golden image's startup scripts, or does OADP trigger it?
4. **Validation** — the cnv-scenarios `check.sh` has 13 validation phases for Windows. Which subset does OADP need? (SSH, OS version, services running, disk count, disk utilization)
5. **Process completion** — HammerDB exits as a scheduled task. How to detect completion? (`Get-Process` via SSH? Task scheduler status?)

Reference: `/tmp/cnv-scenarios/database/hammerdb-mssql/` for the existing Windows pattern.
