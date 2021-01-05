package nosql.batch.update.aerospike.lock;

import nosql.batch.update.lock.PermanentLockingException;

import java.util.List;

public interface AerospikeExpectedValuesOperations<EV>{

    void checkExpectedValues(List<AerospikeLock> locks, EV expectedValues) throws PermanentLockingException;

}
