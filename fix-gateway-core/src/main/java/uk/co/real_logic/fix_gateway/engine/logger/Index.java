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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Different indexes to be run on the archiver implement this interface.
 *
 * Extends {@link FragmentHandler} so that it can be easily used to replay/catchup
 * a Stream.
 */
public interface Index extends FragmentHandler, AutoCloseable
{
    default void onFragment(DirectBuffer buffer, int offset, int length, Header header)
    {
        indexRecord(
            buffer,
            offset,
            length,
            header.streamId(),
            header.sessionId(),
            header.position());
    }

    /**
     * Index a record from an aeron stream.
     *
     * @param buffer buffer where the record is stored.
     * @param offset offset within the buffer.
     * @param length length of the data record within the buffer.
     * @param streamId the Aeron stream Id of the data
     * @param aeronSessionId the Aeron session id.
     * @param position the position in the aeron stream of the start of the buffer
     */
    void indexRecord(final DirectBuffer buffer,
                     final int offset,
                     final int length,
                     final int streamId,
                     final int aeronSessionId,
                     final long position);

    default String getName()
    {
        return getClass().getSimpleName();
    }

    void close();

    /**
     * Reads the last position that has been indexed.
     *
     * @param consumer a callback that receives each session id and position
     */
    void readLastPosition(final IndexedPositionConsumer consumer);
}
