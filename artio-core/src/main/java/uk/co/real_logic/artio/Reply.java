package uk.co.real_logic.artio;

import uk.co.real_logic.artio.engine.FixEngine;

import java.util.concurrent.locks.LockSupport;

public interface Reply<T>
{
    /**
     * Performs a blocking wait on a reply. This can be used for waiting for the reply of administrative operations
     * within Artio. This shouldn't be used on a normal duty cycle as it will stop you from performing different
     * operation concurrently, but can be a useful convenience for operations like {@link FixEngine#bind()} which
     * don't normally need to compose with other replies.
     *
     * @param reply the reply to wait for.
     * @param <T> the type of the object returned by the reply.
     * @return the reply
     */
    static <T> Reply<T> await(final Reply<T> reply)
    {
        while (reply.isExecuting())
        {
            LockSupport.parkNanos(1_000_000);
        }
        return reply;
    }

    default boolean isExecuting()
    {
        return state() == State.EXECUTING;
    }

    default boolean hasTimedOut()
    {
        return state() == State.TIMED_OUT;
    }

    default boolean hasErrored()
    {
        return state() == State.ERRORED;
    }

    default boolean hasCompleted()
    {
        return state() == State.COMPLETED;
    }

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
