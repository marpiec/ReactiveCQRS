# ReactiveCQRS
CQRS framework implementation leveraging Scala and Akka framework, for building reactive distributed applications.


# it uses Default singleton ScalikeJDBC connection pool, so it has to be initialized first


# TODO
- Projection rebuild
- Sagas based on events - no, instead idempotent sagas commands
- Event bus database writes optimization
- Handle OptimisticLockingFailed
- Common transaction for document stores
