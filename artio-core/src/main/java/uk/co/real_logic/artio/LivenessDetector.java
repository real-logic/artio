/*
 * Copyright 2015-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio;

import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.NotConnectedException;
import uk.co.real_logic.artio.util.CharFormatter;

import static uk.co.real_logic.artio.LogTag.LIBRARY_CONNECT;

/**
 * Bidirection application level liveness detector.
 *
 * For use between an engine and a library.
 */
public final class LivenessDetector
{
    public static final int SEND_INTERVAL_FRACTION = 4;

    private static final Runnable NONE = () -> {};

    private static final int AWAITING_CONNECT = 0;
    private static final int CONNECTED = 1;
    private static final int DISCONNECTED = 2;

    private final GatewayPublication publication;
    private final Runnable onDisconnect;
    private final EpochNanoClock clock;
    private final int libraryId;
    private final long replyTimeoutInMs;
    private final long sendIntervalInMs;
    private final CharFormatter disconnectTriggered = new CharFormatter(
        "%s: Disconnect triggered by a NotConnectedException (Stream CLOSED or MAX_POSITION_EXCEEDED)");

    private long latestNextReceiveTimeInMs;
    private long nextSendTimeInMs;
    private int state;

    public static LivenessDetector forEngine(
        final GatewayPublication publication,
        final int libraryId,
        final long replyTimeoutInMs,
        final long timeInMs,
        final EpochNanoClock clock)
    {
        final LivenessDetector detector = new LivenessDetector(
            publication, libraryId, replyTimeoutInMs, CONNECTED, NONE, clock);
        detector.latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
        detector.heartbeat(timeInMs);
        return detector;
    }

    public static LivenessDetector forLibrary(
        final GatewayPublication publication,
        final int libraryId,
        final long replyTimeoutInMs,
        final Runnable onDisconnect,
        final EpochNanoClock clock)
    {
        return new LivenessDetector(
            publication, libraryId, replyTimeoutInMs, AWAITING_CONNECT, onDisconnect, clock);
    }

    private LivenessDetector(
        final GatewayPublication publication,
        final int libraryId,
        final long replyTimeoutInMs,
        final int state,
        final Runnable onDisconnect,
        final EpochNanoClock clock)
    {
        this.publication = publication;
        this.libraryId = libraryId;
        this.replyTimeoutInMs = replyTimeoutInMs;
        this.state = state;
        this.sendIntervalInMs = replyTimeoutInMs / SEND_INTERVAL_FRACTION;
        this.onDisconnect = onDisconnect;
        this.clock = clock;
    }

    public boolean isConnected()
    {
        return state == CONNECTED;
    }

    public boolean hasDisconnected()
    {
        return state == DISCONNECTED;
    }

    public int poll(final long timeInMs)
    {
        switch (state)
        {
            case CONNECTED:
                if (timeInMs > latestNextReceiveTimeInMs)
                {
                    disconnect();
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

    private void disconnect()
    {
        state = DISCONNECTED;
        onDisconnect.run();
    }

    public void onHeartbeat(final long timeInMs)
    {
        if (state != CONNECTED)
        {
            state = CONNECTED;
        }

        latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
    }

    // Reset the connect timer for liveness purposes, but not to flip into the connected state
    public void onConnectStep(final long timeInMs)
    {
        latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
    }

    private void heartbeat(final long timeInMs)
    {
        try
        {
            if (publication.saveApplicationHeartbeat(libraryId, clock.nanoTime()) >= 0)
            {
                nextSendTimeInMs = timeInMs + sendIntervalInMs;
            }
        }
        catch (final NotConnectedException ex)
        {
            if (DebugLogger.isEnabled(LIBRARY_CONNECT))
            {
                DebugLogger.log(LIBRARY_CONNECT, disconnectTriggered.clear().with(libraryId));
            }

            disconnect();
        }
    }
}
