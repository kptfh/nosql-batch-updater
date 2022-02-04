package nosql.batch.update.reactor.aerospike.lock;


import com.aerospike.client.Key;
import nosql.batch.update.aerospike.lock.AerospikeLock;
import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import nosql.batch.update.reactor.aerospike.lock.AerospikeReactorLockOperations.LockResult;
import org.junit.Test;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeReactorLockOperationsTest {

    @Test
    public void shouldSuccess(){

        Key key1 = new Key("ns", "set", "1");
        Key key2 = new Key("ns", "set", "2");

        List<LockResult<AerospikeLock>> lockResults = Arrays.asList(
                new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, key1)),
                new LockResult<>(new AerospikeLock(Lock.LockType.SAME_BATCH, key2)));

        List<AerospikeLock> locked = AerospikeReactorLockOperations.processResults(lockResults).block();
        assertThat(locked).containsExactly(
                new AerospikeLock(Lock.LockType.LOCKED, key1),
                new AerospikeLock(Lock.LockType.SAME_BATCH, key2));
    }

    @Test(expected = TemporaryLockingException.class)
    public void shouldFail(){

        Key keyLocked = new Key("ns", "set", "1");

        List<LockResult<AerospikeLock>> lockResults = Arrays.asList(
                new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, keyLocked)),
                new LockResult<>(new TemporaryLockingException("test")));

        AerospikeReactorLockOperations.processResults(lockResults).block();
    }

    @Test(expected = RuntimeException.class)
    public void shouldSelectNonLockingError(){

        Key keyLocked = new Key("ns", "set", "1");

        List<LockResult<AerospikeLock>> lockResults = Arrays.asList(
                new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, keyLocked)),
                new LockResult<>(new TemporaryLockingException("test")),
                new LockResult<>(new SocketTimeoutException("test")));

        AerospikeReactorLockOperations.processResults(lockResults).block();
    }


}
