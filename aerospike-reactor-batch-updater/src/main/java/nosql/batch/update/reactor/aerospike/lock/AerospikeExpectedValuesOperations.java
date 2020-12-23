package nosql.batch.update.reactor.aerospike.lock;

import nosql.batch.update.reactor.lock.PermanentLockingException;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AerospikeExpectedValuesOperations<EV>{

    Mono<Void> checkExpectedValues(List<AerospikeLock> locks, EV expectedValues) throws PermanentLockingException;

}
