package nosql.batch.update.reactor.aerospike.basic;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import nosql.batch.update.reactor.BatchUpdate;
import nosql.batch.update.reactor.aerospike.basic.lock.AerospikeBasicBatchLocks;
import nosql.batch.update.reactor.aerospike.wal.AerospikeBatchUpdateSerde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aerospike.client.Value.get;

public class AerospikeBasicBatchUpdateSerde
        implements AerospikeBatchUpdateSerde<AerospikeBasicBatchLocks, List<Record>, List<Record>> {

    private static final String EXPECTED_VALUES_BIN_NAME = "expected_values";
    private static final String UPDATES_BIN_NAME = "updates";

    @Override
    public List<Bin> write(BatchUpdate<AerospikeBasicBatchLocks, List<Record>> batch) {
        return Arrays.asList(
                new Bin(EXPECTED_VALUES_BIN_NAME, recordsToValue(batch.locks().expectedValues())),
                new Bin(UPDATES_BIN_NAME, recordsToValue(batch.updates())));
    }

    @Override
    public BatchUpdate<AerospikeBasicBatchLocks, List<Record>> read(Map<String, Object> bins) {
        return new AerospikeBasicBatchUpdate(
                new AerospikeBasicBatchLocks(recordsFromValue(bins.get(EXPECTED_VALUES_BIN_NAME))),
                recordsFromValue(bins.get(UPDATES_BIN_NAME)));
    }

    private static Value recordsToValue(List<Record> records){
        return get(records.stream()
                .map(AerospikeBasicBatchUpdateSerde::recordToValue)
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
        List listOfRecords = (List) value;
        List<Record> records = new ArrayList<>(listOfRecords.size());
        for(Object record : listOfRecords){
            records.add(recordFromValues((List<Object>) record));
        }
        return records;
    }

    private static Record recordFromValues(List<Object> recordValue){
        Iterator<Object> it = recordValue.iterator();
        Key key = new Key((String) it.next(),
                (String) it.next(),
                get(it.next()));
        List<Bin> bins = new ArrayList<>((recordValue.size() - 3) / 2);
        while (it.hasNext()) {
            bins.add(new Bin((String)it.next(), Value.get(it.next())));
        }

        return new Record(key, bins);
    }
}
