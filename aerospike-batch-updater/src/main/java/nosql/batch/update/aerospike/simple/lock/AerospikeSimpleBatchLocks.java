package nosql.batch.update.aerospike.simple.lock;

import com.aerospike.client.Key;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.aerospike.simple.Record;

import java.util.List;
import java.util.stream.Collectors;

public class AerospikeSimpleBatchLocks implements AerospikeBatchLocks<List<Record>> {

    private final List<Record> records;

    public AerospikeSimpleBatchLocks(List<Record> records) {
        this.records = records;
    }

    @Override
    public List<Key> keysToLock() {
        return records.stream()
                .map(record -> toLockKey(record.key))
                .collect(Collectors.toList());
    }

    @Override
    public List<Record> expectedValues() {
        return records;
    }

    public static Key toLockKey(Key key){
        return new Key(key.namespace, key.setName + ".lock", key.userKey);
    }
}
