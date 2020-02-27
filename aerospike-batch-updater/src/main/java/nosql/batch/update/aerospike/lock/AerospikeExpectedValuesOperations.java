package nosql.batch.update.aerospike.lock;

import nosql.batch.update.lock.PermanentLockingException;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AerospikeExpectedValuesOperations<EV>{

    Mono<Void> checkExpectedValues(List<AerospikeLock> locks, EV expectedValues) throws PermanentLockingException;

}
