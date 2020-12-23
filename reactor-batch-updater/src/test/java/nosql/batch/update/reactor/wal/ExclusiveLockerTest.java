package nosql.batch.update.reactor.wal;

import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

abstract public class ExclusiveLockerTest {

    abstract protected ExclusiveLocker getExclusiveLocker();

    private ExclusiveLocker exclusiveLocker = getExclusiveLocker();

    @After
    public void after(){
        exclusiveLocker.shutdown();
    }

    @Test
    public void shouldBeReentrant(){
        assertThat(exclusiveLocker.acquire()).isTrue();
        assertThat(exclusiveLocker.acquire()).isTrue();

        exclusiveLocker.release();
    }

    @Test
    public void shouldBeExclusive() {
        ExclusiveLocker exclusiveLocker2 = getExclusiveLocker();

        assertThat(exclusiveLocker.acquire()).isTrue();
        assertThat(exclusiveLocker2.acquire()).isFalse();

        exclusiveLocker.release();
        exclusiveLocker2.shutdown();
    }

    @Test
    public void shouldLockAfterUnlock(){
        ExclusiveLocker exclusiveLocker2 = getExclusiveLocker();

        assertThat(exclusiveLocker.acquire()).isTrue();
        exclusiveLocker.release();
        assertThat(exclusiveLocker2.acquire()).isTrue();

        exclusiveLocker2.release();
        exclusiveLocker2.shutdown();
    }

}
