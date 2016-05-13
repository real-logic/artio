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

public abstract class Reply<T>
{
    private enum State
    {
        EXECUTING,
        TIMED_OUT,
        ERRORED,
        COMPLETED
    }

    private final long latestReplyArrivalTime;

    private Exception error;
    private T result;
    private State state;

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

    public Exception error()
    {
        return error;
    }

    public T resultIfPresent()
    {
        return result;
    }

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
