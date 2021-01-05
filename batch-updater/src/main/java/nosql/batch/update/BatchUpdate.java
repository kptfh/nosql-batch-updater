package nosql.batch.update;

public interface BatchUpdate <LOCKS, UPDATES>{

    /**
     * @return Locks that should be acquired before applying batch of updated.
     *         After locks acquired we should check for expected values if post lock approach used
     */
    LOCKS locks();
    UPDATES updates();
}
