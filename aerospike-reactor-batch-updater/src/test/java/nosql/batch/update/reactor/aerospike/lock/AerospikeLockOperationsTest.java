package nosql.batch.update.reactor.aerospike.lock;


import com.aerospike.client.Key;
import nosql.batch.update.reactor.aerospike.lock.AerospikeLockOperations.LockResult;
import nosql.batch.update.reactor.lock.Lock;
import nosql.batch.update.reactor.lock.PermanentLockingException;
import nosql.batch.update.reactor.lock.TemporaryLockingException;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeLockOperationsTest {

    @Test
    public void shouldSuccess(){

        Key key1 = new Key("ns", "set", "1");
        Key key2 = new Key("ns", "set", "2");

        List<LockResult<AerospikeLock>> lockResults = Arrays.asList(
                new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, key1)),
                new LockResult<>(new AerospikeLock(Lock.LockType.SAME_BATCH, key2)));

        List<AerospikeLock> locked = AerospikeLockOperations.processResults(new Object(), o -> Mono.empty(), lockResults).block();
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

        AerospikeLockOperations.processResults(new Object(), o -> Mono.empty(), lockResults).block();
    }

    @Test(expected = PermanentLockingException.class)
    public void shouldSelectNonLockingError(){

        Key keyLocked = new Key("ns", "set", "1");

        List<LockResult<AerospikeLock>> lockResults = Arrays.asList(
                new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, keyLocked)),
                new LockResult<>(new TemporaryLockingException("test")),
                new LockResult<>(new SocketTimeoutException("test")));

        AerospikeLockOperations.processResults(new Object(), o -> Mono.empty(), lockResults).block();
    }


}
