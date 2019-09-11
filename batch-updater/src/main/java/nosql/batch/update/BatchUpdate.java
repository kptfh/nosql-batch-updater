package nosql.batch.update;

public interface BatchUpdate <L, U>{

    /**
     * @return Locks that should be acquired before applying batch of updated.
     *         After locks acquired we should check for expected values
     */
    L locks();
    U updates();
}
