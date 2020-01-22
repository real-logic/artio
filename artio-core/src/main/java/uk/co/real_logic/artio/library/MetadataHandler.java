/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.library;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.messages.MetaDataStatus;

/**
 * Callback to indicate the read of a metadata piece of metadata for a session.
 */
@FunctionalInterface
public interface MetadataHandler
{
    /**
     * Called when the metadata has been read. The buffer with this data should not be assumed to exist outside of the
     * lifetime of this callback. You should copy the data inside the callback if you want to to use it afterwards.
     *
     * If the status of the callback is not OK the length will be 0 and the read has resulted in an error.
     *
     * @param sessionId the id of the session that this metadata has been written
     * @param status whether the read has been successful or resulted in an error.
     * @param buffer buffer with the metadata in.
     * @param offset offset within the buffer.
     * @param length length of the data within the buffer.
     */
    void onMetaData(
        long sessionId,
        MetaDataStatus status,
        DirectBuffer buffer,
        int offset,
        int length);
}
