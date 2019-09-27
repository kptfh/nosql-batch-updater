package nosql.batch.update.aerospike.simple;

import nosql.batch.update.BatchUpdate;
import nosql.batch.update.aerospike.simple.lock.AerospikeSimpleBatchLocks;

import java.util.List;

public class AerospikeSimpleBatchUpdate implements BatchUpdate<AerospikeSimpleBatchLocks, List<Record>> {

    private final AerospikeSimpleBatchLocks locks;
    private final List<Record> updates;

    public AerospikeSimpleBatchUpdate(AerospikeSimpleBatchLocks locks, List<Record> updates) {
        this.locks = locks;
        this.updates = updates;
    }

    @Override
    public AerospikeSimpleBatchLocks locks() {
        return locks;
    }

    @Override
    public List<Record> updates() {
        return updates;
    }
}
