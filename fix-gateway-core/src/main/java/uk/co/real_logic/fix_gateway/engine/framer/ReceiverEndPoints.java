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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.ArrayUtil;
import uk.co.real_logic.agrona.nio.TransportPoller;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.stream.Stream;

import static uk.co.real_logic.agrona.collections.ArrayUtil.UNKNOWN_INDEX;

public class ReceiverEndPoints extends TransportPoller
{
    private ReceiverEndPoint[] endPoints = new ReceiverEndPoint[0];

    public void add(final ReceiverEndPoint endPoint)
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

    public void removeConnection(final long connectionId)
    {
        final ReceiverEndPoint[] endPoints = this.endPoints;
        final int length = endPoints.length;
        int index = UNKNOWN_INDEX;

        for (int i = 0; i < length; i++)
        {
            final ReceiverEndPoint endPoint = endPoints[i];
            if (endPoint.connectionId() == connectionId)
            {
                index = i;
                endPoint.close();
            }
        }

        this.endPoints = ArrayUtil.remove(endPoints, index);
    }

    public void removeLibrary(final int libraryId)
    {
        final ReceiverEndPoint[] endPoints = this.endPoints;
        final int length = endPoints.length;
        int removeCount = 0;

        for (int i = 0; i < length; i++)
        {
            final ReceiverEndPoint endPoint = endPoints[i];
            if (endPoint.libraryId() == libraryId)
            {
                endPoint.close();
                removeCount++;
            }
        }

        if (removeCount > 0)
        {
            final int newLength = length - removeCount;
            final ReceiverEndPoint[] newEndPoints = ArrayUtil.newArray(endPoints, newLength);

            for (int i = 0, j = 0; i < length; i++)
            {
                final ReceiverEndPoint endPoint = endPoints[i];
                if (endPoint.libraryId() != libraryId)
                {
                    newEndPoints[j] = endPoint;
                    j++;
                }
            }

            this.endPoints = newEndPoints;
        }
    }

    public int pollEndPoints()
    {
        int bytesReceived = 0;
        try
        {
            final ReceiverEndPoint[] endPoints = this.endPoints;
            final int numEndPoints = endPoints.length;
            if (numEndPoints <= ITERATION_THRESHOLD)
            {
                for (int i = numEndPoints - 1; i >= 0; i--)
                {
                    bytesReceived += endPoints[i].pollForData();
                }
            }
            else
            {
                selector.selectNow();

                final SelectionKey[] keys = selectedKeySet.keys();
                for (int i = selectedKeySet.size() - 1; i >= 0; i--)
                {
                    bytesReceived += ((ReceiverEndPoint)keys[i].attachment()).pollForData();
                }

                selectedKeySet.reset();
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return bytesReceived;
    }

    public void close()
    {
        Stream.of(endPoints).forEach(ReceiverEndPoint::close);
        super.close();
    }
}
