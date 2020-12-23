package nosql.batch.update.reactor.aerospike.basic;

import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import nosql.batch.update.reactor.aerospike.lock.AerospikeExpectedValuesOperations;
import nosql.batch.update.reactor.aerospike.lock.AerospikeLock;
import nosql.batch.update.reactor.lock.Lock;
import nosql.batch.update.reactor.lock.PermanentLockingException;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;


public class AerospikeBasicExpectedValueOperations implements AerospikeExpectedValuesOperations<List<Record>> {

    private final IAerospikeReactorClient client;

    public AerospikeBasicExpectedValueOperations(IAerospikeReactorClient client) {
        this.client = client;
    }

    @Override
    public Mono<Void> checkExpectedValues(List<AerospikeLock> locks, List<Record> expectedValues) throws PermanentLockingException {

        if(locks.size() != expectedValues.size()){
            throw new IllegalArgumentException("locks.size() != expectedValues.size()");
        }

        List<BatchRead> batchReads = new ArrayList<>(expectedValues.size());
        List<Record> expectedValuesToCheck = new ArrayList<>(expectedValues.size());
        for(int i = 0, n = expectedValues.size(); i < n; i++){
            if(locks.get(i).lockType == Lock.LockType.SAME_BATCH){
                continue;
            }
            Record record = expectedValues.get(i);
            batchReads.add(new BatchRead(record.key, record.bins.stream()
                    .map(bin -> bin.name)
                    .toArray(String[]::new)));
            expectedValuesToCheck.add(record);
        }

        return client.get(null, batchReads)
                .doOnNext(batchReadResult -> {
                    for(int i = 0, n = expectedValuesToCheck.size(); i < n; i++){
                        checkValues(batchReads.get(i), expectedValuesToCheck.get(i));
                    }
                })
                .then();


    }

    private void checkValues(BatchRead batchRead, Record expectedValues) throws PermanentLockingException {
        for(Bin bin : expectedValues.bins){
            Object actualValue = batchRead.record != null ? batchRead.record.getValue(bin.name) : null;
            if(!equals(actualValue, bin.value)){
                throw new PermanentLockingException(String.format(
                        "Unexpected value: bin=[%s], expected=[%s], actual=[%s]",
                        bin.name, bin.value, actualValue));
            }
        }
    }

    private boolean equals(Object actualValue, Value expectedValue) {
         return expectedValue.equals(Value.get(actualValue));
    }
}
