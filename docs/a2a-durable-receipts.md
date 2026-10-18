# Durable A2A Task Receipts

Carrier: EMAIL-SWEEP-A2A-TIMEOUT-20261016-NOON. No original task may be replayed to test receipt retrieval.

The receiver commits acceptance to `<profile>/a2a_tasks/receipts.sqlite3` before dispatch. A message-ID binding includes authenticated peer, served agent, tenant, exact context and a content fingerprint. A repeated binding returns the original task; conflicting reuse fails closed. The dedicated directory is 0700 and database 0600. Terminal results are immutable and are not evicted after 500 tasks. This does not alter or migrate the session database.

After process replacement, retained submitted/working tasks are exposed as input-required with outcome unknown and an original-owner reconciliation warning. They are not rescheduled, failed, canceled, or marked completed. Completion needs authentic original-owner evidence; the archive does not reconstruct missing historical receipts from conversations or context proximity.

Outbound JSON-RPC correlation IDs are not receiver task IDs. Clients persist the observed mapping only after validating the returned context. Alias lookup is scoped to configured peer and endpoint/tenant. Unknown legacy aliases are not guessed. A result for a different task/context is rejected. Alias persistence failure preserves the returned canonical receipt and warns not to resend. The live one-second read-only 429 retry remains unchanged.

`GetTask` with `metadataOnly: true` omits history, artifacts and status message. It still returns the canonical task ID, context and typed state. Authenticated health exposes aggregate receipt backend/retention data. The standalone inventory never dispatches or reads results:

```sh
python -m plugins.platforms.a2a.task_archive
```

An absent archive is an explicit durability coverage gap, not a healthy empty result. Metadata inventory does not create one. Do not interpret a task transport state as proof of business action completion.

## Rollout

Diff every adapter/client against the deployed file before installation. Preserve host-specific routing/security and owner hotfixes. Install the archive module before consumers. Keep timestamped verified backups; rollback only if current digests still equal the installed candidate, retaining all archive data. No gateway or messaging restart is part of this change. On-disk parity and a fresh-process synthetic installed canary do not prove a long-lived receiver has loaded the change; activation is a separate gate. A mixed messaging gateway cannot be restarted to clear that gate here.

Historical accepted IDs whose records predate this archive remain original-owner reconciliation cases. Do not create a replacement task/receipt, release source claims, or manufacture a terminal outcome. The old noon scheduler run's missing durable termination and scheduled-producer adoption are independent gates.
