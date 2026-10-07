# Cloud Run sandbox — experimental draft

This implements the **tool-execution half** of [RFC 76](https://github.com/openclaw/rfcs/pull/76), not durable Gateway hosting. It uses the Google preview launcher inside an already provisioned Cloud Run container. It does not provision cloud resources, elevate itself, or change the standard OpenClaw Dockerfile.

## Lifecycle and filesystem

- Each command owns a unique named guest. Creation runs only an idle process; untrusted code launches separately after creation and current-authority checks.
- Cancellation, interrupt, timeout, preparation failure and normal finalization delete that exact guest with `sandbox delete --force`. Killing the launcher alone is insufficient. Concurrent commands have separate guests.
- Core owns runtime generations. A plugin-scoped SQLite journal owns their child guests, recorded before allocation. No expiry or eviction is allowed; allocation fails at 10,000 outstanding receipts.
- Failed deletion retains its receipt. Recovery deletes acknowledged orphan names. An interrupted or failed creation remains **unsettled**: an absent name does not prove a late create cannot publish it. This draft fails closed and still needs a provider settlement contract for automatic recovery. Stop/recreate the enclosing Cloud Run container and reconcile its receipt before reuse; never erase receipts while the container may still run.
- Root overlays are ephemeral. Only selected workspace mounts persist between commands. Background children are removed on command completion; separate commands cannot share a guest-root server. A finite guest lifetime (default 600 seconds, maximum 3600) also limits long commands.
- A clean, independently built root filesystem is mandatory. Never use `/`, Gateway state, credentials, or a copy of the live host filesystem. Read-only is **not** secret. The root and its ancestors must be root-owned, not group/world writable.
- Egress defaults off. Enabling it is broad outbound access, not a domain allowlist; review metadata, private-network and service-identity exposure first.
- Explicit environment values are staged via stdin into the fresh guest, not launcher arguments. Parent environment is not copied into the guest.

## Prerequisites

Enable Google sandbox support on the enclosing Cloud Run workload. Workspace binds and egress failed from UID 1000 in the October 7 experiment. This draft requires Linux/root in a **dedicated experimental image**; the standard image keeps `USER node`.

Build the guest root separately at image-build time, for example from a minimal Python image. It needs POSIX shell, sleep, env, Python 3, filesystem tools, dynamic loader and libraries. Do not copy live mounted secrets, Gateway state or runtime credentials into it. Merely copying executable files is insufficient.

This private plugin is not published to npm or ClawHub. Use a source/linked development install in an isolated test Gateway:

```json5
{
  plugins: {
    entries: {
      "cloud-run-sandbox": {
        enabled: true,
        config: {
          rootfs: "/opt/openclaw-sandbox-rootfs",
          allowEgress: false,
          guestLifetimeSeconds: 600,
        },
      },
    },
  },
  agents: {
    defaults: {
      sandbox: {
        mode: "all",
        backend: "cloud-run-sandbox",
        workspaceAccess: "none",
      },
    },
  },
}
```

Workspace selection follows core: `none` exposes only its private workspace; `ro` uses read-only mounts; `rw` permits selected workspace writes. A distinct authorized agent workspace maps to `/agent`. All host sources must be disjoint when either mount is writable. This draft **rejects nested skill/instruction sources inside a writable workspace**, including ordinary generated skill layouts, rather than leaving replaceable paths or writable aliases. Disjoint read-only resource sources are supported. Supporting the usual writable-workspace-plus-nested-skills layout requires a provider-supported pinned mount owner; do not disable the guard. Arbitrary Docker binds, setupCommand, managed-project projections, guest PTYs and sandboxed browsers are unsupported; there is no fallback to host execution.

**Live component checks (October 7, 2026):** separate `run`/`exec` with a clean root worked; repeated `--mount` flags preserved a writable workspace and a read-only agent mount; a parent private-file canary was hidden; forced deletion after a started marker prevented the delayed completion write. `do` with the tested custom root failed, so this adapter deliberately does not use it. Three bounded, one-task/zero-retry Cloud Run Jobs and their no-role service accounts were deleted after testing.

**Promotion gate:** those are provider-component checks, not a full current-plugin/agent end-to-end certification. Protected nested mounts, hostile path replacement, crash recovery and the full lifecycle matrix still require validation and independent security review. Do not use this draft with real secrets or hostile workloads before that gate passes.

## Hosting and storage are separate

The actual Gateway initialized with local state on Cloud Run in the October 7 experiment. That does not certify durable state across replacement, authenticated external ingress, channels, upgrades or a model-driven end-to-end turn.

**Do not mount the live OpenClaw state directory on GCS FUSE.** The actual Gateway failed configuration publication/file-identity checks and logged write-order errors involving SQLite, journals and shared-memory files. A successful text counter and tiny SQLite smoke did not establish application filesystem semantics. This is not a claim that every SQLite operation fails. No NFS or object-sync substitute is certified here.

Google billing budgets are alerts, not hard caps. Live validation needs bounded task duration, one task, zero retries, minimal/no-role service identity, bounded instance count, an outer watchdog and verified deletion. This plugin is not a billing-cap mechanism.

## Tests

```sh
pnpm test extensions/cloud-run-sandbox
pnpm check:changed
```

Before promotion, verify none/ro/rw and protected skills; parent file/env canaries; args/stdin/env and file read/write/readback; workdir/symlink escapes; cancellation after a started marker without a later marker; concurrent guests; authority revocation during create/staging; timeout and normal descendant teardown; ambiguous create, deletion retry and crash recovery.

References: [Google code execution](https://docs.cloud.google.com/run/docs/code-execution), [sandbox CLI](https://docs.cloud.google.com/run/docs/reference/sandbox-cli), [GCS FUSE limitations](https://docs.cloud.google.com/storage/docs/cloud-storage-fuse/overview#limitations).
