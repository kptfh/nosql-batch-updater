package nosql.batch.update.wal;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

abstract public class WriteAheadLogManagerTest<BATCH_ID> {

    abstract protected BATCH_ID saveBatch();

    abstract protected boolean removeBatch(BATCH_ID batchId);

    abstract protected void switchClockAhead();

    abstract protected List<BATCH_ID> getStaleBatches();

    @Test
    public void shouldRetrieveStaleBatches(){
        BATCH_ID batchId = saveBatch();

        List<BATCH_ID> staleBatches = getStaleBatches();
        assertThat(staleBatches).isEmpty();

        switchClockAhead();
        staleBatches = getStaleBatches();
        assertThat(staleBatches).containsExactly(batchId);
    }

    @Test
    public void shouldDeleteBatch(){
        BATCH_ID batchId = saveBatch();
        assertThat(removeBatch(batchId)).isTrue();
        assertThat(removeBatch(batchId)).isFalse();
    }


}
