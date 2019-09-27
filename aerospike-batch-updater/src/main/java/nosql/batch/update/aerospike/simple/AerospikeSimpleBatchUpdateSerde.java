package nosql.batch.update.aerospike.simple;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import nosql.batch.update.BatchUpdate;
import nosql.batch.update.aerospike.simple.lock.AerospikeSimpleBatchLocks;
import nosql.batch.update.aerospike.wal.AerospikeBatchUpdateSerde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aerospike.client.Value.get;

public class AerospikeSimpleBatchUpdateSerde
        implements AerospikeBatchUpdateSerde<AerospikeSimpleBatchLocks, List<Record>, List<Record>> {

    private static final String EXPECTED_VALUES_BIN_NAME = "expected_values";
    private static final String UPDATES_BIN_NAME = "updates";

    @Override
    public List<Bin> write(BatchUpdate<AerospikeSimpleBatchLocks, List<Record>> batch) {
        return Arrays.asList(
                new Bin(EXPECTED_VALUES_BIN_NAME, recordsToValue(batch.locks().expectedValues())),
                new Bin(UPDATES_BIN_NAME, recordsToValue(batch.updates())));
    }

    @Override
    public BatchUpdate<AerospikeSimpleBatchLocks, List<Record>> read(Map<String, Object> bins) {
        return new AerospikeSimpleBatchUpdate(
                new AerospikeSimpleBatchLocks(recordsFromValue(bins.get(EXPECTED_VALUES_BIN_NAME))),
                recordsFromValue(bins.get(UPDATES_BIN_NAME)));
    }

    private static Value recordsToValue(List<Record> records){
        return get(records.stream()
                .map(AerospikeSimpleBatchUpdateSerde::recordToValue)
                .collect(Collectors.toList()));
    }

    private static Value recordToValue(Record record){
        List<Value> recordValues = new ArrayList<>();
        recordValues.add(get(record.key.namespace));
        recordValues.add(get(record.key.setName));
        recordValues.add(record.key.userKey);
        for(Bin bin : record.bins){
            recordValues.add(get(bin.name));
            recordValues.add(bin.value);
        }
        return get(recordValues);
    }

    private static List<Record> recordsFromValue(Object value){
        //TODO implement
        throw new UnsupportedOperationException();
    }
}
