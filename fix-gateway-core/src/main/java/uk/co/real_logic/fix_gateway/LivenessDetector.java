/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.fix_gateway.streams.GatewayPublication;

/**
 * Bidirection application level liveness detector.
 *
 * For use between an engine and a library.
 */
public class LivenessDetector
{
    private final GatewayPublication publication;
    private final int libraryId;
    private final long replyTimeoutInMs;
    private final long sendIntervalInMs;

    private long latestNextReceiveTimeInMs;
    private long nextSendTimeInMs;
    private boolean isConnected;

    public LivenessDetector(
        final GatewayPublication publication,
        final int libraryId,
        final long replyTimeoutInMs,
        final long timeInMs,
        final boolean isConnected)
    {
        this.publication = publication;
        this.libraryId = libraryId;
        this.replyTimeoutInMs = replyTimeoutInMs;
        this.isConnected = isConnected;
        this.sendIntervalInMs = replyTimeoutInMs / 2;

        if (isConnected)
        {
            latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
        }

        heartbeat(timeInMs);
    }

    public boolean isConnected()
    {
        return isConnected;
    }

    public int poll(final long timeInMs)
    {
        if (isConnected)
        {
            if (timeInMs > latestNextReceiveTimeInMs)
            {
                isConnected = false;
                return 1;
            }

            if (timeInMs > nextSendTimeInMs)
            {
                heartbeat(timeInMs);
                return 1;
            }
        }

        return 0;
    }

    public void onHeartbeat(final long timeInMs)
    {
        if (!isConnected)
        {
            isConnected = true;
        }
        latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
    }

    private void heartbeat(final long timeInMs)
    {
        nextSendTimeInMs = timeInMs + sendIntervalInMs;
        publication.saveApplicationHeartbeat(libraryId);
    }
}
