package nosql.batch.update.reactor.aerospike.basic;

import nosql.batch.update.reactor.BatchUpdate;
import nosql.batch.update.reactor.aerospike.basic.lock.AerospikeBasicBatchLocks;

import java.util.List;

public class AerospikeBasicBatchUpdate implements BatchUpdate<AerospikeBasicBatchLocks, List<Record>> {

    private final AerospikeBasicBatchLocks locks;
    private final List<Record> updates;

    public AerospikeBasicBatchUpdate(AerospikeBasicBatchLocks locks, List<Record> updates) {
        this.locks = locks;
        this.updates = updates;
    }

    @Override
    public AerospikeBasicBatchLocks locks() {
        return locks;
    }

    @Override
    public List<Record> updates() {
        return updates;
    }
}
