# Linux OS Variant Profiles

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Golden Image Lifecycle](002-golden-image-lifecycle.md)

## Question

Create VM profiles for additional Linux OS variants beyond RHEL9: CentOS Stream, Fedora, Ubuntu. Each needs:

1. Profile YAML with correct image source URL
2. Golden image in appropriate format (qcow2)
3. Cloud-init compatibility verification (all these support cloud-init but may differ in package manager, service names)
4. Validation: same flow as RHEL9 — SSH, disk mount, data capacity checks

Resolve: at least 2 additional OS variant profiles created and validated with the golden image pipeline.
