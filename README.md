# Graph repository over a public KV Port

`KvGraphRepository::new(Arc<dyn KvStore>)` implements public C0 Data graph
storage over a caller-provided KV backend. It maintains forward/reverse indexes,
metadata, pagination and projection revision/tombstone fences. Atomic projection
updates require the backend's public batch compare-and-swap capability; missing
capabilities and storage failures remain errors.

This library does not select a World, tenant, backend, key namespace or canonical
store. Callers isolate the supplied KV namespace and retain semantic acceptance,
authorization and lifecycle policy. The adapter's transaction methods retain the
existing per-operation behavior; they do not promise a multi-call transaction.

The initial implementation and existing regression tests come from
`hvac-workbench` commit `04b106a8467975276bfe70577b3d1e824521492e`, `apps/adapters/univers-storage-kv-graph`.
The source has no implementation changes beyond its public package import and
corrected documentation of the required KV capabilities.

Add `univers-aip-lib-storage-kv-graph = {version="=0.1.0",registry="univers"}`.
The release lock validates C0 Data rc.2. Run `bash scripts/check.sh`,
`bash scripts/build.sh`, and `bash scripts/publish.sh` from an unchanged clean
committed candidate. Registry configuration is included; credentials are external.
