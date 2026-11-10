# SSH Key Secret Management

**Label:** `wayfinder:task`  
**Status:** Open  
**Blocked by:** [Script-Based Scenarios and Unit Tests](023-script-based-scenarios-and-tests.md)

## Question

Add native SSH key secret management to `vm_operations.py`, matching the pattern in `VMsPopulation.sh` `GetOrCreateAutomationKey()`. The scripts use ed25519 key pairs stored in a Kubernetes Secret for `virtctl ssh` access; the Python pipeline currently has no equivalent and relies on password-based auth or placeholder SSH key config.

Implementation:

1. Add `_create_ssh_key_secret(namespace)` to `vm_operations.py`:
   - Check if Secret `oadp-automation-sshkey` exists in namespace via `oc get secret`
   - If exists: extract `id_ed25519` and `id_ed25519.pub` from Secret data (base64 decode), write to temp files
   - If absent: generate ed25519 key pair (`ssh-keygen -t ed25519`), create Secret with both keys
   - Return paths to private and public key files
   - Temp key files cleaned up via `atexit` or context manager

2. Inject public key into VM cloud-init:
   - Update `_batch_create_vms()` to pass the public key as `ssh_authorized_keys` in the template render
   - Update `vm_template.yaml` cloud-init block to include the key (already supports `ssh_authorized_keys` list)

3. Use private key for `virtctl ssh` operations:
   - Update `_populate_vms_with_data()` to use `--identity-file=<private_key>` with `virtctl ssh`
   - Update `_validate_single_vm_data()` in `vm_validation.py` similarly
   - Add `--local-ssh-opts="-o StrictHostKeyChecking=no"` and `--local-ssh-opts="-o UserKnownHostsFile=/dev/null"` matching the script pattern

4. Deprecate password-based auth:
   - The scripts use `100yard-` password as fallback; the Python pipeline should prefer key-based auth
   - Keep password in profile as optional fallback for environments without `ssh-keygen`

Resolve: key management implemented, VMs accessible via key-based `virtctl ssh`, unit tests cover key creation and reuse paths.
