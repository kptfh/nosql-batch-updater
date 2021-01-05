package nosql.batch.update.reactor.aerospike.basic;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.aerospike.basic.Record;
import nosql.batch.update.reactor.ReactorUpdateOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class AerospikeBasicReactorUpdateOperations implements ReactorUpdateOperations<List<Record>> {

    private final IAerospikeReactorClient client;
    private final WritePolicy writePolicy;

    public AerospikeBasicReactorUpdateOperations(IAerospikeReactorClient client) {
        this.client = client;
        this.writePolicy = client.getWritePolicyDefault();
    }

    @Override
    public Mono<Void> updateMany(List<Record> batchOfUpdates, boolean calledByWal) {
        return Flux.fromIterable(batchOfUpdates)
                .flatMap(this::update)
                .then();
   }

    private Mono<Key> update(Record record){
        return client.put(writePolicy, record.key, record.bins.toArray(new Bin[0]));
    }
}
