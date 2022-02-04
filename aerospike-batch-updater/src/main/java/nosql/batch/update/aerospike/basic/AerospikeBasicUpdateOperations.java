package nosql.batch.update.aerospike.basic;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import nosql.batch.update.UpdateOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;

public class AerospikeBasicUpdateOperations implements UpdateOperations<List<Record>> {

    private final IAerospikeClient client;
    private final WritePolicy writePolicy;
    private final ExecutorService executorService;

    public AerospikeBasicUpdateOperations(IAerospikeClient client, ExecutorService executorService) {
        this.client = client;
        this.writePolicy = client.getWritePolicyDefault();
        this.executorService = executorService;
    }

    @Override
    public void updateMany(List<Record> batchOfUpdates, boolean calledByWal) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(batchOfUpdates.size());
        for(Record record : batchOfUpdates){
            futures.add(runAsync(() -> update(record), executorService));
        }
        allOf(futures.toArray(new CompletableFuture<?>[0])).join();
   }

    private void update(Record record){
        client.put(writePolicy, record.key, record.bins.toArray(new Bin[0]));
    }
}
