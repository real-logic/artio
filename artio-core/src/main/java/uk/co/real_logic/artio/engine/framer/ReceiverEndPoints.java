/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.nio.TransportPoller;
import uk.co.real_logic.artio.messages.DisconnectReason;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.stream.Stream;

import static org.agrona.collections.ArrayUtil.UNKNOWN_INDEX;
import static uk.co.real_logic.artio.messages.DisconnectReason.ENGINE_SHUTDOWN;

class ReceiverEndPoints extends TransportPoller
{
    // Authentication flow requires periodic polling of the receiver end points until the authentication is
    // complete, so these endpoints are always polled, rather than using the selector.
    private ReceiverEndPoint[] requiredPollingEndPoints = new ReceiverEndPoint[0];
    private ReceiverEndPoint[] endPoints = new ReceiverEndPoint[0];

    void add(final ReceiverEndPoint endPoint)
    {
        if (endPoint.requiresAuthentication())
        {
            requiredPollingEndPoints = ArrayUtil.add(requiredPollingEndPoints, endPoint);
        }
        else
        {
            addToNormalEndpoints(endPoint);
        }
    }

    private void addToNormalEndpoints(final ReceiverEndPoint endPoint)
    {
        try
        {
            endPoints = ArrayUtil.add(endPoints, endPoint);
            endPoint.register(selector);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    void removeConnection(final long connectionId, final DisconnectReason reason)
    {
        final ReceiverEndPoint[] endPoints = this.endPoints;
        int index = findAndCloseEndPoint(connectionId, reason, endPoints);

        if (index != UNKNOWN_INDEX)
        {
            this.endPoints = ArrayUtil.remove(endPoints, index);
        }
        else
        {
            index = findAndCloseEndPoint(connectionId, reason, requiredPollingEndPoints);
            this.requiredPollingEndPoints = ArrayUtil.remove(requiredPollingEndPoints, index);
        }

        selectNowToForceProcessing();
    }

    private int findAndCloseEndPoint(
        final long connectionId,
        final DisconnectReason reason,
        final ReceiverEndPoint[] endPoints)
    {
        int index = UNKNOWN_INDEX;
        final int length = endPoints.length;
        for (int i = 0; i < length; i++)
        {
            final ReceiverEndPoint endPoint = endPoints[i];
            if (endPoint.connectionId() == connectionId)
            {
                index = i;
                endPoint.close(reason);
            }
        }
        return index;
    }

    private void selectNowToForceProcessing()
    {
        try
        {
            selector.selectNow();
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    int pollEndPoints()
    {
        int bytesReceived = 0;
        try
        {
            ReceiverEndPoint[] requiredPollingEndPoints = this.requiredPollingEndPoints;
            final ReceiverEndPoint[] endPoints = this.endPoints;

            final int numRequiredPollingEndPoints = requiredPollingEndPoints.length;
            final int numEndPoints = endPoints.length;
            final int threshold = ITERATION_THRESHOLD - numRequiredPollingEndPoints;
            if (numEndPoints <= threshold)
            {
                for (int i = numEndPoints - 1; i >= 0; i--)
                {
                    bytesReceived += endPoints[i].poll();
                }
            }
            else
            {
                selector.selectNow();

                final SelectionKey[] keys = selectedKeySet.keys();
                for (int i = selectedKeySet.size() - 1; i >= 0; i--)
                {
                    bytesReceived += ((ReceiverEndPoint)keys[i].attachment()).poll();
                }

                selectedKeySet.reset();
            }

            for (int i = numRequiredPollingEndPoints - 1; i >= 0; i--)
            {
                final ReceiverEndPoint requiredPollingEndPoint = requiredPollingEndPoints[i];
                bytesReceived += requiredPollingEndPoint.poll();
                if (!requiredPollingEndPoint.requiresAuthentication())
                {
                    requiredPollingEndPoints = ArrayUtil.remove(requiredPollingEndPoints, i);
                    addToNormalEndpoints(requiredPollingEndPoint);
                }
            }
            this.requiredPollingEndPoints = requiredPollingEndPoints;
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return bytesReceived;
    }

    public void close()
    {
        Stream.of(endPoints).forEach(receiverEndPoint -> receiverEndPoint.close(ENGINE_SHUTDOWN));
        super.close();
    }
}
