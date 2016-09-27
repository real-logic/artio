package uk.co.real_logic.fix_gateway;

public interface Reply<T>
{
    boolean isExecuting();

    boolean hasTimedOut();

    boolean hasErrored();

    boolean hasCompleted();

    /**
     * Gets the error iff <code>hasErrored() == true</code> or null otherwise.
     *
     * @return the error iff <code>hasErrored() == true</code> or null otherwise.
     */
    Exception error();

    /**
     * Gets the result if the operation has completed successfully or null.
     *
     * @return the result if the operation has completed successfully or null.
     */
    T resultIfPresent();

    /**
     * Gets the current state of the Reply.
     *
     * @return the current state of the Reply.
     */
    State state();

    /**
     * Gets the correlation id of the message that is being replied to.
     *
     * @return the correlation id of the message that is being replied to.
     */
    long correlationId();

    enum State
    {
        /** The operation is currently being executed and its result is unknown. */
        EXECUTING,
        /** The operation has timed out without a result. */
        TIMED_OUT,
        /** The operation has completed with an error. */
        ERRORED,
        /** The operation has completed successfully. */
        COMPLETED
    }
}
