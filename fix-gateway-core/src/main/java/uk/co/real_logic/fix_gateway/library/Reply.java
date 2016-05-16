/*
 * Copyright 2015-2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.library;

import uk.co.real_logic.fix_gateway.messages.GatewayError;

/**
 * Represents a reply from an asynchronous method. Methods can complete successfully, in error
 * or they can timeout.
 *
 * This class isn't threadsafe and should be used on the same thread as the FixLibrary instance.
 *
 * @param <T> the return type of the method in question.
 */
public abstract class Reply<T>
{
    public enum State
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

    private final long latestReplyArrivalTime;

    private Exception error;
    private T result;
    private State state = State.EXECUTING;

    Reply(final long latestReplyArrivalTime)
    {
        this.latestReplyArrivalTime = latestReplyArrivalTime;
    }

    public boolean isExecuting()
    {
        return state == State.EXECUTING;
    }

    public boolean hasTimedOut()
    {
        return state == State.TIMED_OUT;
    }

    public boolean hasErrored()
    {
        return state == State.ERRORED;
    }

    public boolean hasCompleted()
    {
        return state == State.COMPLETED;
    }

    /**
     * Gets the error iff <code>hasErrored() == true</code> or null otherwise.
     *
     * @return the error iff <code>hasErrored() == true</code> or null otherwise.
     */
    public Exception error()
    {
        return error;
    }

    /**
     * Gets the result if the operation has completed successfully or null.
     *
     * @return the result if the operation has completed successfully or null.
     */
    public T resultIfPresent()
    {
        return result;
    }

    /**
     * Gets the current state of the Reply.
     *
     * @return the current state of the Reply.
     */
    public State state()
    {
        return state;
    }

    void onComplete(T result)
    {
        this.result = result;
        state = State.COMPLETED;
    }

    void onError(final Exception error)
    {
        this.error = error;
        state = State.ERRORED;
    }

    abstract void onError(final GatewayError errorType, final String errorMessage);

    boolean checkTimeout(final long time)
    {
        if (time >= latestReplyArrivalTime)
        {
            state = State.TIMED_OUT;
            return true;
        }

        return false;
    }
}
