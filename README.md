# Kroxylicious Filter for Kafka Connect API interactions

A filter based on the [Kroxylicious sample filter](https://github.com/kroxylicious/kroxylicious/tree/main/kroxylicious-sample) that observes the following API interactions:

* FindCoordinatorRequest
* FindCoordinatorResponse
* JoinGroupRequest
* JoinGroupResponse
* SyncGroupRequest
* SyncGroupResponse
* HeartbeatResponse (if an error code is included)

## Build the Filter

Build the filter with:

```shell
mvn package
```

## Run the Filter

To run the filter execute the following command, replacing the PATH-TO-KROXYLICIOUS with your Kroxylicious install:
```shell
KROXYLICIOUS_CLASSPATH="kroxylicious-connect-filter/target/kroxylicious-connect-filter-0.0.1-jar-with-dependencies.jar" /PATH-TO-KROXYLICIOUS/bin/kroxylicious-start.sh --config proxy-config-1.yaml
```

You can then start Kafka Connect with `bootstrap.servers=localhost:9192`.

To run a second worker, start a second Kroxylicious with:
```shell
KROXYLICIOUS_CLASSPATH="kroxylicious-connect-filter/target/kroxylicious-connect-filter-0.0.1-jar-with-dependencies.jar" /PATH-TO-KROXYLICIOUS/bin/kroxylicious-start.sh --config proxy-config-2.yaml
```

The second Kafka Connect worker should have `bootstrap.servers=localhost:9195`.
