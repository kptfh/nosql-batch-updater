package nosql.batch.update.lock;

abstract public class Lock {

    public final LockType lockType;

    protected Lock(LockType lockType) {
        this.lockType = lockType;
    }

    enum LockType {
        /*Locked in scope of the transaction*/
        LOCKED,

        /**Signals that was already locked in scope of interrupted transaction.
        Used only by WriteAheadLogCompleter*/
        SAME_TRANSACTION
    }
}
