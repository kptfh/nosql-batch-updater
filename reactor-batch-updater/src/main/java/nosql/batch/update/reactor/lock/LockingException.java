package nosql.batch.update.reactor.lock;

abstract public class LockingException extends RuntimeException{

    public LockingException(Throwable cause){
        super(cause);
    }

    public LockingException(String message){
        super(message);
    }

}
