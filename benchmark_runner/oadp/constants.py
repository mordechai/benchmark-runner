"""
Centralized constants for the OADP workload module.

All magic strings, hardcoded paths, default values, and external
script references are collected here to avoid scattering them
across multiple files.
"""

# ---------------------------------------------------------------------------
# External script paths (mpqe-scale-scripts repository)
# ---------------------------------------------------------------------------
MPQE_BUSYBOX_PATH = "/tmp/mpqe-scale-scripts/mtc-helpers/busybox"
MPQE_OADP_BASE_DIR = "/tmp/mpqe-scale-scripts/oadp-helpers"
MPQE_MISC_DIR = "/tmp/mpqe-scale-scripts/misc-scripts"
MPQE_SCENARIO_DATA = "/tmp/mpqe-scale-scripts/oadp-helpers/templates/internal_data/tests.yaml"
MPQE_PROMQL_QUERIES = "/tmp/mpqe-scale-scripts/oadp-helpers/templates/metrics/metrics-oadp.yaml"

MPQE_VM_SCRIPTS_PATH = "/tmp/mpqe-scale-scripts/oadp-helpers/Scripts"

# VMsPopulation.sh's multi-NS loop always appends "-ns<i>" to the namespace
# base name, even when only one namespace is requested (-ns 1, the only
# value benchmark-runner passes today). Script-based VM scenarios must set
# their dataset/args namespace to end with this suffix; vm_operations strips
# it before passing the base name via -namespace.
MPQE_VM_SCRIPT_NS_SUFFIX = "-ns1"

# ---------------------------------------------------------------------------
# Velero CLI paths
# ---------------------------------------------------------------------------
VELERO_CLI_PATH = "/tmp/velero-1-12"

# ---------------------------------------------------------------------------
# Report / artifact paths
# ---------------------------------------------------------------------------
RESULT_REPORT_PATH = "/tmp/oadp-report.json"
PREVIOUS_REPORT_PATH = "/tmp/previous-run-oadp-report.json"

# ---------------------------------------------------------------------------
# OpenShift namespaces
# ---------------------------------------------------------------------------
DEFAULT_VELERO_NS = "openshift-adp"
OPENSHIFT_STORAGE_NS = "openshift-storage"
MINIO_BUCKET_NS = "minio-bucket"
OPENSHIFT_CONSOLE_NS = "openshift-console"
OPENSHIFT_MARKETPLACE_NS = "openshift-marketplace"
OPENSHIFT_OPERATORS_NS = "openshift-operators"

# ---------------------------------------------------------------------------
# DPA (Data Protection Application) defaults
# ---------------------------------------------------------------------------
DEFAULT_DPA_NAME = "example-velero"

# ---------------------------------------------------------------------------
# MinIO defaults
# ---------------------------------------------------------------------------
MINIO_DEFAULT_ACCESS_KEY = "minio"
MINIO_DEFAULT_SECRET_KEY = "minio123"

# ---------------------------------------------------------------------------
# Elasticsearch index prefixes
# ---------------------------------------------------------------------------
ES_INDEX_PREFIX_UPSTREAM = "velero-"
ES_INDEX_PREFIX_DOWNSTREAM = "oadp-"

# ---------------------------------------------------------------------------
# Stream source identifiers
# ---------------------------------------------------------------------------
SOURCE_UPSTREAM = "upstream"
SOURCE_DOWNSTREAM = "downstream"

# ---------------------------------------------------------------------------
# Validation modes
# ---------------------------------------------------------------------------
VALIDATION_MODE_NONE = "none"
VALIDATION_MODE_LIGHT = "light"
VALIDATION_MODE_FULL = "full"

# ---------------------------------------------------------------------------
# CR terminal states (backup/restore)
# ---------------------------------------------------------------------------
CR_TERMINAL_STATES = frozenset(
    {
        "Completed",
        "Failed",
        "PartiallyFailed",
        "Deleted",
        "FinalizingPartiallyFailed",
        "WaitingForPluginOperationsPartiallyFailed",
    }
)

CR_AUTH_ERROR_STATES = frozenset(
    {
        "couldn't get current server",
        "forbidden",
        "Unauthorized",
        "(Unauthorized)",
    }
)

# ---------------------------------------------------------------------------
# Plugin identifiers
# ---------------------------------------------------------------------------
PLUGIN_RESTIC = "restic"
PLUGIN_KOPIA = "kopia"
PLUGIN_CSI = "csi"
PLUGIN_VBD = "vbd"
PLUGIN_VSM = "vsm"
PLUGIN_KUBEVIRT = "kubevirt"

# File-level backup plugins (use fs-backup flags)
FILE_LEVEL_PLUGINS = frozenset({PLUGIN_RESTIC, PLUGIN_KOPIA})

# All recognized plugin identifiers for fail-fast validation
KNOWN_PLUGINS = frozenset({PLUGIN_RESTIC, PLUGIN_KOPIA, PLUGIN_CSI, PLUGIN_VBD, PLUGIN_VSM, PLUGIN_KUBEVIRT})

# DPA defaultPlugins entry for CBT-enabled kubevirt backups (Flow2, ticket 028).
# NOT a valid value for the scenario's `args.plugin` field (which stays
# "kubevirt" in both flows) — this is only ever checked/patched against the
# DPA's `spec.configuration.velero.defaultPlugins` list.
PLUGIN_KUBEVIRT_DATAMOVER = "kubevirt-datamover"

# Resource-policy ConfigMap referenced by CBT-enabled kubevirt backups (Flow2,
# ticket 028) via `velero backup create --resource-policies-configmap` /
# `spec.resourcePolicy` on the backup CR. Policy action is
# `type: custom` / `datamover: kubevirt` so matching volumes are routed
# through the kubevirt datamover instead of Velero's CSI-snapshot handling,
# avoiding "PVC ... has snapshot policy which conflicts with kubevirt
# datamover" backup warnings.
KUBEVIRT_VOLUME_POLICY_CONFIGMAP_NAME = "kubevirt-volume-policy"

# ---------------------------------------------------------------------------
# Timestamp formats
# ---------------------------------------------------------------------------
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
CR_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# ---------------------------------------------------------------------------
# Storage classes
# ---------------------------------------------------------------------------
SC_CEPH_RBD = "ocs-storagecluster-ceph-rbd"
SC_CEPH_RBD_VIRTUALIZATION = "ocs-storagecluster-ceph-rbd-virtualization"
SC_CEPHFS = "ocs-storagecluster-cephfs"
SC_CEPHFS_SHALLOW = "ocs-storagecluster-cephfs-shallow"

# ---------------------------------------------------------------------------
# KubeVirt / VM defaults
# ---------------------------------------------------------------------------
VM_DATASET_ROLE = "kubevirt"
VM_DEFAULT_BATCH_SIZE = 10
VM_DEFAULT_THROTTLE_BATCHES = 5
VM_DEFAULT_POPULATION_CONCURRENCY = 10
VM_DEFAULT_SSH_USER = "cloud-user"

# VM population method: "native" drives the Python pipeline (create_vm_dataset),
# "script" drives the mpqe-scale-scripts shell scripts (create_vm_dataset_via_script).
VM_POPULATION_METHOD_NATIVE = "native"
VM_POPULATION_METHOD_SCRIPT = "script"
KNOWN_VM_POPULATION_METHODS = frozenset({VM_POPULATION_METHOD_NATIVE, VM_POPULATION_METHOD_SCRIPT})

# VMsAddDataVerify.sh increment levels (see ticket 022)
KNOWN_INCREMENT_LEVELS = frozenset({"inc1", "inc2", "inc3"})

# Characters forbidden in scenario string values that flow into shell commands
SHELL_METACHARACTERS = frozenset(";|&$`(){}><\n\r")

# ---------------------------------------------------------------------------
# Retry / timeout defaults
# ---------------------------------------------------------------------------
DEFAULT_RETRY_INTERVAL = 15
DEFAULT_RETRY_MAX_ATTEMPTS = 20
DEFAULT_TESTCASE_TIMEOUT = 43200
DELETION_TIMEOUT = 900
BSL_MAX_RETRIES = 30
