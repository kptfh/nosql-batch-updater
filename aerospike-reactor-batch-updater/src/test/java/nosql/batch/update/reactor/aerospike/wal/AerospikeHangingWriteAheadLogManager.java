package nosql.batch.update.reactor.aerospike.wal;

import com.aerospike.client.Value;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.reactor.wal.ReactorHangingWriteAheadLogManager;
import nosql.batch.update.reactor.wal.ReactorWriteAheadLogManager;

import java.util.concurrent.atomic.AtomicBoolean;

public class AerospikeHangingWriteAheadLogManager<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
        extends ReactorHangingWriteAheadLogManager<LOCKS, UPDATES, Value> {

    public AerospikeHangingWriteAheadLogManager(ReactorWriteAheadLogManager<LOCKS, UPDATES, Value> writeAheadLogManager, AtomicBoolean failsDelete) {
        super(writeAheadLogManager, failsDelete);
    }

    public static <LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
    AerospikeHangingWriteAheadLogManager<LOCKS, UPDATES, EV> hangingWal(
            AerospikeReactorWriteAheadLogManager<LOCKS, UPDATES, EV> writeAheadLogManager, AtomicBoolean failsDelete){
        return new AerospikeHangingWriteAheadLogManager<>(writeAheadLogManager, failsDelete);
    }
}
