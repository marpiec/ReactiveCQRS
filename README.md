# ReactiveCQRS
CQRS framework implementation leveraging Scala and Akka framework, for building reactive distributed applications.


# it uses Default singleton ScalikeJDBC connection pool, so it has to be initialized first


# TODO
- Projection rebuild
- Event bus database writes optimization
- Handle OptimisticLockingFailed
- extract class_names to separate table - to reduce tables size
- Common transaction for document stores
