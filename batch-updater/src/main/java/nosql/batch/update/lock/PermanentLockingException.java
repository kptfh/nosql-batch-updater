package nosql.batch.update.lock;

/**
 * Thrown if expected value check failed
 * Indicates that batch update may not be retried
 */
public class PermanentLockingException extends LockingException{

    public PermanentLockingException(Throwable cause){
        super(cause);
    }

    public PermanentLockingException(String message){
        super(message);
    }

}
