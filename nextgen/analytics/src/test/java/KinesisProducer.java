import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import org.junit.Test;

import java.nio.ByteBuffer;

public class KinesisProducer {
    @Test
    public void insertRecords() {

        final AmazonKinesisClient client = new AmazonKinesisClient(new BasicAWSCredentials("AKIAJBZM3CD7FH7GYDQA", "i+bPtKd1tmXyK27UZBxaYn3Nl0M29CqHubbRnqd3"));
        client.setEndpoint("https://kinesis.us-west-2.amazonaws.com");
        client.setRegion(Region.getRegion(Regions.US_WEST_2));

        String sequenceNumberOfPreviousRecord;
        for (int j = 0; j < 100; j++) {
            final PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName("dlabs");
            putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d", j).getBytes()));
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", j % 5));
            putRecordRequest.setSequenceNumberForOrdering("1");
            PutRecordResult putRecordResult = client.putRecord(putRecordRequest);
            sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();
            System.out.println(sequenceNumberOfPreviousRecord);
        }
    }
}

// epic
// data - events ( start a job/ end a job) // job ID, NodeID, tenet ID, Fauciltiy ID

// display - Node ID, time to process, cycle times of each job, aggregates, Process Time, Work In progress,

// epic
// digesting
// -- dcd stream, digester to handle it, dcd steam
// epic --  on demand stream

// epic: spit out something for dash boards, DCD/ DIQ 2.0 like dash boards

// DLABS - dashboards
// one or a few of these guys
// T- admin UI