package nosql.batch.update.aerospike.basic;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import nosql.batch.update.UpdateOperations;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;

public class AerospikeBasicUpdateOperations implements UpdateOperations<List<Record>> {

    private final IAerospikeClient client;
    private final WritePolicy writePolicy;

    public AerospikeBasicUpdateOperations(IAerospikeClient client) {
        this.client = client;
        this.writePolicy = client.getWritePolicyDefault();
    }

    @Override
    public void updateMany(List<Record> batchOfUpdates) {
        allOf(batchOfUpdates.stream()
                .map(record -> runAsync(() -> update(record)))
                .toArray(CompletableFuture[]::new)).join();

    }

    private void update(Record record){
        client.put(writePolicy, record.key, record.bins.toArray(new Bin[0]));
    }
}
