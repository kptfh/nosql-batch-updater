package nosql.batch.update.reactor.aerospike.lock;

import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.PermanentLockingException;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AerospikeReactorExpectedValuesOperations<EV>{

    Mono<Void> checkExpectedValues(List<AerospikeLock> locks, EV expectedValues) throws PermanentLockingException;

}
