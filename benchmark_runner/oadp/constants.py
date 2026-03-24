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

# File-level backup plugins (use fs-backup flags)
FILE_LEVEL_PLUGINS = frozenset({PLUGIN_RESTIC, PLUGIN_KOPIA})

# ---------------------------------------------------------------------------
# Timestamp formats
# ---------------------------------------------------------------------------
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
CR_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# ---------------------------------------------------------------------------
# Storage classes
# ---------------------------------------------------------------------------
SC_CEPH_RBD = "ocs-storagecluster-ceph-rbd"
SC_CEPHFS = "ocs-storagecluster-cephfs"
SC_CEPHFS_SHALLOW = "ocs-storagecluster-cephfs-shallow"

# ---------------------------------------------------------------------------
# Retry / timeout defaults
# ---------------------------------------------------------------------------
DEFAULT_RETRY_INTERVAL = 15
DEFAULT_RETRY_MAX_ATTEMPTS = 20
DEFAULT_TESTCASE_TIMEOUT = 43200
DELETION_TIMEOUT = 900
BSL_MAX_RETRIES = 30
