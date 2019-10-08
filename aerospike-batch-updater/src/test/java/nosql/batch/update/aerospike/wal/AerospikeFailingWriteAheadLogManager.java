package nosql.batch.update.aerospike.wal;

import com.aerospike.client.Value;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.wal.FailingWriteAheadLogManager;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.util.concurrent.atomic.AtomicBoolean;

public class AerospikeFailingWriteAheadLogManager<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
        extends FailingWriteAheadLogManager<LOCKS, UPDATES, Value> {

    public AerospikeFailingWriteAheadLogManager(WriteAheadLogManager<LOCKS, UPDATES, Value> writeAheadLogManager, AtomicBoolean failsDelete) {
        super(writeAheadLogManager, failsDelete);
    }

    public static <LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
    AerospikeFailingWriteAheadLogManager<LOCKS, UPDATES, EV> failingWal(
            AerospikeWriteAheadLogManager<LOCKS, UPDATES, EV> writeAheadLogManager, AtomicBoolean failsDelete){
        return new AerospikeFailingWriteAheadLogManager<>(writeAheadLogManager, failsDelete);
    }
}
