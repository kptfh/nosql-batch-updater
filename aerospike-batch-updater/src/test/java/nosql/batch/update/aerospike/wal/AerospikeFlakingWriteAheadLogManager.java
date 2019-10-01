package nosql.batch.update.aerospike.wal;

import com.aerospike.client.Value;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.wal.FlakingWriteAheadLogManager;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.util.concurrent.atomic.AtomicBoolean;

public class AerospikeFlakingWriteAheadLogManager<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
        extends FlakingWriteAheadLogManager<LOCKS, UPDATES, Value> {

    public AerospikeFlakingWriteAheadLogManager(WriteAheadLogManager<LOCKS, UPDATES, Value> writeAheadLogManager, AtomicBoolean failsDelete) {
        super(writeAheadLogManager, failsDelete);
    }

    public static <LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
    AerospikeFlakingWriteAheadLogManager<LOCKS, UPDATES, EV> flakingWal(
            AerospikeWriteAheadLogManager<LOCKS, UPDATES, EV> writeAheadLogManager, AtomicBoolean failsDelete){
        return new AerospikeFlakingWriteAheadLogManager<>(writeAheadLogManager, failsDelete);
    }
}
