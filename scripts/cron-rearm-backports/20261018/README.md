# Cron Rearm Fleet Backports

These source-preserving patches carry the reviewed recurring-resume contract
from `6252ebf0cf6490ea3afb29c7cf6419a75efdb416` into installed runtime families.
They are not whole-runtime upgrades. The JSON manifests fence every input and
output hash; any live mismatch requires reconciliation before installation.

Use the shared `cron/exhaustion.py` at the behavior-source revision. Apply a
family patch only in a code-only candidate, verify all output hashes, run its
native cron tests, then install only changed manifest paths with timestamped
backups and compare-and-swap checks. Test the installed CLI with isolated HOME
and HERMES_HOME. Never run or revive production jobs for verification.

Watson's existing external-worker self-removal receipt implementation and CLI
doctor diagnostics remain intact. The CLI already propagates failure status in
this family, so its main module is an unchanged parity dependency.

Long-lived scheduler activation, business-open classification of exhausted
monitors, and owner review are separate gates. File hashes or isolated tests do
not prove a pre-existing process loaded the new module. Jeff's replacement
watcher must remain the sole active watcher for its script.
