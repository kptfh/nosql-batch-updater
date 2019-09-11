package nosql.batch.update.wal;

public interface ExclusiveLocker {

    boolean acquireExclusiveLock();
    boolean renewExclusiveLock();
}
