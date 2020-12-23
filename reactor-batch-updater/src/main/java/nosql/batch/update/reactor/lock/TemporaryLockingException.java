package nosql.batch.update.reactor.lock;

/**
 * Thrown if some locks already locked by concurrent batch update
 * Indicates that batch update may be retried later
 */
public class TemporaryLockingException extends LockingException{

    public TemporaryLockingException(String message){
        super(message);
    }


}
