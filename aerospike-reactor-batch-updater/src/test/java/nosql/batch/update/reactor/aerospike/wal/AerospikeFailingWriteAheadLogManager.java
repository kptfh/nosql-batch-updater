package nosql.batch.update.reactor.aerospike.wal;

import com.aerospike.client.Value;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.reactor.wal.ReactorFailingWriteAheadLogManager;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AerospikeFailingWriteAheadLogManager<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
        extends ReactorFailingWriteAheadLogManager<LOCKS, UPDATES, Value> {

    public AerospikeFailingWriteAheadLogManager(ReactorWriteAheadLogManager<LOCKS, UPDATES, Value> writeAheadLogManager,
                                                AtomicBoolean failsDelete, AtomicInteger deletesInProcess) {
        super(writeAheadLogManager, failsDelete, deletesInProcess);
    }

    public static <LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
    AerospikeFailingWriteAheadLogManager<LOCKS, UPDATES, EV> failingWal(
            AerospikeReactorWriteAheadLogManager<LOCKS, UPDATES, EV> writeAheadLogManager,
            AtomicBoolean failsDelete, AtomicInteger deletesInProcess){
        return new AerospikeFailingWriteAheadLogManager<>(writeAheadLogManager, failsDelete, deletesInProcess);
    }
}
