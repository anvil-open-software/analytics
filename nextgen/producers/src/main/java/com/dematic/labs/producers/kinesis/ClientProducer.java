package com.dematic.labs.producers.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.dematic.labs.toolkit.aws.Connections;

import javax.enterprise.inject.Produces;

public final class ClientProducer {
    //todo: will come back to this when nextgen starts starts to push events
    @Produces
    public static AWSCredentialsProvider getAWSCredentialsProvider() {
        return Connections.getAWSCredentialsProvider();
    }
}
