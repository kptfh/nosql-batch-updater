package nosql.batch.update.reactor.aerospike.wal;

import com.aerospike.client.Value;
import nosql.batch.update.reactor.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.reactor.wal.FailingWriteAheadLogManager;
import nosql.batch.update.reactor.wal.WriteAheadLogManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AerospikeFailingWriteAheadLogManager<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
        extends FailingWriteAheadLogManager<LOCKS, UPDATES, Value> {

    public AerospikeFailingWriteAheadLogManager(WriteAheadLogManager<LOCKS, UPDATES, Value> writeAheadLogManager,
                                                AtomicBoolean failsDelete, AtomicInteger deletesInProcess) {
        super(writeAheadLogManager, failsDelete, deletesInProcess);
    }

    public static <LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
    AerospikeFailingWriteAheadLogManager<LOCKS, UPDATES, EV> failingWal(
            AerospikeWriteAheadLogManager<LOCKS, UPDATES, EV> writeAheadLogManager,
            AtomicBoolean failsDelete, AtomicInteger deletesInProcess){
        return new AerospikeFailingWriteAheadLogManager<>(writeAheadLogManager, failsDelete, deletesInProcess);
    }
}
