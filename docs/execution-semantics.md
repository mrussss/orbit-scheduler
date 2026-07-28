# Execution semantics

Orbit provides at-least-once execution. A task may execute more than once, but
only the current `worker_instance_id` and monotonic `attempt_no` may renew its
lease or commit its result. Terminal task states never regress.

Cancellation is cooperative. A running task receives a cancellation request on
lease renewal; a result that committed before the cancellation condition wins.

