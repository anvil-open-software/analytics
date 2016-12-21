dlab
====

Dematic Labs on demand solution for analytics using Spark, kafka, cassandra.

## Deployment Implications During Code Review
When code reviewing analytics module, if any of the following changed.

#### Driver name and package
Drivers get deployed based on the meta data definition passed into ansible via jenkins.
Currently keys have to be unique.

#### Jar size
Please make sure Jar size did not get ballooned with unnecessary inclusions.

