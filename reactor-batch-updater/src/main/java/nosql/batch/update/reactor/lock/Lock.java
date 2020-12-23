package nosql.batch.update.reactor.lock;

abstract public class Lock {

    public final LockType lockType;

    protected Lock(LockType lockType) {
        this.lockType = lockType;
    }

    public enum LockType {
        /*Locked in scope of the batch update*/
        LOCKED,

        /**Signals that was already locked in scope of interrupted batch update.
           Used only by WriteAheadLogCompleter*/
        SAME_BATCH
    }
}
