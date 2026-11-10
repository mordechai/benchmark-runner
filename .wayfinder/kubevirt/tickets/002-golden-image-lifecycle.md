# Golden Image Lifecycle Implementation

**Label:** `wayfinder:grilling`  
**Status:** Closed  
**Blocked by:** [Profile Schema Redesign](001-profile-schema-redesign.md)

## Question

What is the exact implementation flow for golden image check-or-create within `create_vm_dataset()`?

Decisions needed:
1. **Check mechanism** — `oc get dv <name> -n <shared-ns>` and check phase == Succeeded? Or check PVC directly?
2. **Import trigger** — if golden DV is absent, create a DV with `http` source pointing to the qcow2 URL. What timeout for large images (100Gi Windows)?
3. **Concurrent safety** — if two scenarios run simultaneously and both try to import the same golden image, how to handle the race? (CDI may already handle this via DV ownership)
4. **Clone strategy** — each VM's DataVolumeTemplate references the golden DV via `pvc: {namespace: <shared-ns>, name: <golden-name>}` source. Confirm this triggers CDI smart cloning.
5. **Cleanup** — should the golden image DV be deleted on cleanup, or left in place for reuse across runs?
6. **Failure handling** — if golden image import fails (network, bad URL), how to surface the error clearly before attempting VM creation?

## Resolution

### Implementation Flow

```
create_vm_dataset(scenario, ds)
  └─> ensure_golden_image(profile)
        │
        ├─ oc apply -f golden_dv_manifest.yaml    # idempotent — no-op if exists
        │   (DV spec: source.http.url = profile.image.url,
        │    namespace = profile.image.golden_namespace,
        │    name = profile.image.golden_name,
        │    storage.storageClassName = profile.image.storage_class,
        │    storage.volumeMode = profile.image.volume_mode,
        │    storage.resources.requests.storage = profile.image.root_size)
        │
        └─ poll_dv_phase_until_succeeded(
             name = profile.image.golden_name,
             namespace = profile.image.golden_namespace,
             timeout = profile.image.import_timeout_minutes
           )
           │
           ├─ Succeeded → return (golden ready for cloning)
           └─ Failed/Timeout → FAIL FAST with DV status + events
```

### Decisions

| # | Decision | Answer |
|---|----------|--------|
| 1 | Check mechanism | DV phase: `oc get dv <name> -n <ns> -o jsonpath='{.status.phase}'` == `Succeeded` |
| 2 | Import timeout | Configurable: `image.import_timeout_minutes` in profile YAML (adds to schema from ticket #001) |
| 3 | Concurrent safety | `oc apply` is idempotent — second run hits existing DV and just waits for Succeeded. No locking needed. |
| 4 | Clone strategy | VM DataVolumeTemplate uses `source.pvc: {namespace: <golden_ns>, name: <golden_name>}`. CDI automatically selects clone method (smart clone / host-assisted copy / CSI clone based on storage class capabilities). |
| 5 | Cleanup | Never delete golden images. They are shared infrastructure, reused across all runs. Not touched by `cleaning_up_oadp_resources()`. |
| 6 | Failure handling | Fail fast. If DV phase transitions to an error state or timeout expires: collect DV status + events, report clear error message, abort run before any VM creation attempt. |

### Profile Schema Addition

From this ticket, `image.import_timeout_minutes` is added to the profile schema:

```yaml
image:
  url: "http://host/rhel9-hammerdb-mariadb.qcow2"
  golden_namespace: openshift-virtualization-os-images
  golden_name: rhel9-hammerdb-mariadb-golden
  storage_class: ocs-storagecluster-ceph-rbd-virtualization
  volume_mode: Filesystem
  root_size: 30Gi
  import_timeout_minutes: 60    # NEW — how long to wait for CDI import
```

### Golden DV Manifest (generated at runtime)

```yaml
apiVersion: cdi.kubevirt.io/v1beta1
kind: DataVolume
metadata:
  name: {{ profile.image.golden_name }}
  namespace: {{ profile.image.golden_namespace }}
spec:
  source:
    http:
      url: {{ profile.image.url }}
  storage:
    accessModes:
      - ReadWriteOnce
    resources:
      requests:
        storage: {{ profile.image.root_size }}
    storageClassName: {{ profile.image.storage_class }}
    volumeMode: {{ profile.image.volume_mode }}
```

### VM DataVolumeTemplate Clone Pattern

Each VM's root disk clones from the golden:

```yaml
dataVolumeTemplates:
- metadata:
    name: {{ vm_name }}-rootdisk
  spec:
    source:
      pvc:
        namespace: {{ profile.image.golden_namespace }}
        name: {{ profile.image.golden_name }}
    storage:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: {{ profile.image.root_size }}
      storageClassName: {{ profile.image.storage_class }}
      volumeMode: {{ profile.image.volume_mode }}
```

### Key Implementation Notes

- Golden images are **never cleaned up** — add an explicit exclusion in `cleanup.py`
- The `oc apply` approach means a failed import from a prior run leaves a DV in error state. The code should detect `Failed` phase and delete+recreate the DV before re-importing (not a retry in the error-handling sense — just recovering from stale state).
- The golden namespace (`openshift-virtualization-os-images`) must exist before import. If it doesn't, create it.
