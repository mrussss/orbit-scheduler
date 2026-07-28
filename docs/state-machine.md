# Task state machine

```text
PENDING --fetch--> RUNNING --success--> SUCCEEDED
   |                  |  \--permanent/exhausted--> FAILED
   |                  |  \--requested cancel-----> CANCELED
   |                  \----retry/expired lease----> PENDING
   \--cancel--------------------------------------> CANCELED
```

`SUCCEEDED`, `FAILED`, and `CANCELED` are terminal. Every RUNNING transition
increments `attempt_no`; that number is also the fencing token paired with the
unique worker instance UUID.

