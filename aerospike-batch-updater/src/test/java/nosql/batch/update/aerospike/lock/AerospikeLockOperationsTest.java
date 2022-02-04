package nosql.batch.update.aerospike.lock;


import com.aerospike.client.Key;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations.LockResult;
import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.PermanentLockingException;
import nosql.batch.update.lock.TemporaryLockingException;
import org.junit.Test;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeLockOperationsTest {

    @Test
    public void shouldSuccess(){

        Key key1 = new Key("ns", "set", "1");
        Key key2 = new Key("ns", "set", "2");

        List<CompletableFuture<LockResult<AerospikeLock>>> lockResults = Arrays.asList(
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, key1))),
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.SAME_BATCH, key2))));

        List<AerospikeLock> locked = AerospikeLockOperations.processResults(lockResults);
        assertThat(locked).containsExactly(
                new AerospikeLock(Lock.LockType.LOCKED, key1),
                new AerospikeLock(Lock.LockType.SAME_BATCH, key2));
    }

    @Test(expected = TemporaryLockingException.class)
    public void shouldFail(){

        Key keyLocked = new Key("ns", "set", "1");

        List<CompletableFuture<LockResult<AerospikeLock>>> lockResults = Arrays.asList(
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, keyLocked))),
                completedFuture(new LockResult<>(new TemporaryLockingException("test"))));

        AerospikeLockOperations.processResults(lockResults);
    }

    @Test(expected = RuntimeException.class)
    public void shouldSelectNonLockingError(){

        Key keyLocked = new Key("ns", "set", "1");

        List<CompletableFuture<LockResult<AerospikeLock>>> lockResults = Arrays.asList(
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, keyLocked))),
                completedFuture(new LockResult<>(new TemporaryLockingException("test"))),
                completedFuture(new LockResult<>(new SocketTimeoutException("test"))));

        AerospikeLockOperations.processResults(lockResults);
    }


}
