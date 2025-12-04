# px-pem (Standalone Pixie PEM)

This chart deploys a standalone Pixie PEM as a DaemonSet using image `wrongerror/standalone_pem_image:dev` by default.

## Install

```sh
helm install px-pem ./charts/px-pem \
  --namespace pixie --create-namespace \
  --set image.repository=wrongerror/standalone_pem_image \
  --set image.tag=dev
```

## Notes
- Requires privileged container and BPF mounts:
  - `/sys`, `/lib/modules`, `/usr/src`, `/sys/fs/cgroup`, `/sys/fs/bpf`.
- `hostPID: true` enables access to host processes for perf/BPF.
- RBAC grants read-only access to pods/nodes/namespaces; disable via `values.rbac.enabled=false` if not needed.
- Adjust resources and capabilities per your cluster security policies.
