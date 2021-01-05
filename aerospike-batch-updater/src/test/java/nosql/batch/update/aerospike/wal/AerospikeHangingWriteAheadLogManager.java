package nosql.batch.update.aerospike.wal;

import com.aerospike.client.Value;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.wal.HangingWriteAheadLogManager;
import nosql.batch.update.wal.WriteAheadLogManager;

import java.util.concurrent.atomic.AtomicBoolean;

public class AerospikeHangingWriteAheadLogManager<LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
        extends HangingWriteAheadLogManager<LOCKS, UPDATES, Value> {

    public AerospikeHangingWriteAheadLogManager(WriteAheadLogManager<LOCKS, UPDATES, Value> writeAheadLogManager, AtomicBoolean failsDelete) {
        super(writeAheadLogManager, failsDelete);
    }

    public static <LOCKS extends AerospikeBatchLocks<EV>, UPDATES, EV>
    AerospikeHangingWriteAheadLogManager<LOCKS, UPDATES, EV> hangingWal(
            AerospikeWriteAheadLogManager<LOCKS, UPDATES, EV> writeAheadLogManager, AtomicBoolean failsDelete){
        return new AerospikeHangingWriteAheadLogManager<>(writeAheadLogManager, failsDelete);
    }
}
