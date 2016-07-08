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
package uk.co.real_logic.fix_gateway.system_tests;

import org.agrona.LangUtil;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SteppingIdleStrategy implements IdleStrategy
{
    private static final long RUN_FREE_IDLE_TIME_IN_NS = MILLISECONDS.toNanos(1);

    private volatile boolean isStepping = false;
    private final CyclicBarrier barrier = new CyclicBarrier(2);

    public void idle(final int work)
    {
        idle();
    }

    public void idle()
    {
        step();
    }

    public void reset()
    {
    }

    public void step()
    {
        if (isStepping)
        {
            try
            {
                barrier.await();
            }
            catch (InterruptedException | BrokenBarrierException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
        }
        else
        {
            LockSupport.parkNanos(RUN_FREE_IDLE_TIME_IN_NS);
        }
    }

    public void startStepping()
    {
        isStepping = true;
    }

    public void stopStepping()
    {
        isStepping = false;
    }
}
