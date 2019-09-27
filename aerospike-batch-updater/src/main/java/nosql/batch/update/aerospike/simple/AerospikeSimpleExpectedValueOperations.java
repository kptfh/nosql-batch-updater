package nosql.batch.update.aerospike.simple;

import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import nosql.batch.update.aerospike.lock.AerospikeExpectedValuesOperations;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.PermanentLockingException;

import java.util.List;
import java.util.stream.Collectors;


public class AerospikeSimpleExpectedValueOperations implements AerospikeExpectedValuesOperations<List<Record>> {

    private final IAerospikeClient client;

    AerospikeSimpleExpectedValueOperations(IAerospikeClient client) {
        this.client = client;
    }

    @Override
    public void checkExpectedValues(List<AerospikeLock> locks, List<Record> expectedValues) throws PermanentLockingException {

        List<BatchRead> batchReads = expectedValues.stream()
                .map(record -> new BatchRead(record.key, record.bins.stream()
                        .map(bin -> bin.name)
                        .toArray(String[]::new)))
                .collect(Collectors.toList());

        client.get(null, batchReads);

        for(int i = 0, n = expectedValues.size(); i < n; i++){
            checkValues(batchReads.get(i), expectedValues.get(i));
        }
    }

    private void checkValues(BatchRead batchRead, Record expectedValues) throws PermanentLockingException {
        for(Bin bin : expectedValues.bins){
            Object actualValue = batchRead.record != null ? batchRead.record.getValue(bin.name) : null;
            if(!equals(actualValue, bin.value)){
                throw new PermanentLockingException(String.format(
                        "Unexpected value: bin=[%s], expected=[%s], actual=[%s]",
                        bin.name, bin.value, batchRead.record.getValue(bin.name)));
            }
        }
    }

    private boolean equals(Object actualValue, Value expectedValue) {
         return expectedValue.equals(Value.get(actualValue));
    }
}
