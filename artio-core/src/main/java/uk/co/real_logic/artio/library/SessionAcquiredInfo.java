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
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.messages.MetaDataStatus;
import uk.co.real_logic.artio.messages.SlowStatus;

/**
 * Information about the session being acquired.
 *
 * NB: any state in this object is only valid for the lifetime of the
 * {@link SessionAcquireHandler} callback. If your application wishes to use
 * the state after the callback then it should copy it.
 */
public class SessionAcquiredInfo
{
    private MetaDataStatus metaDataStatus;
    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer();
    private boolean isSlow;

    public MetaDataStatus metaDataStatus()
    {
        return metaDataStatus;
    }

    public DirectBuffer metaDataBuffer()
    {
        return metaDataBuffer;
    }

    public boolean isSlow()
    {
        return isSlow;
    }

    void wrap(
        final SlowStatus slowStatus,
        final MetaDataStatus metaDataStatus,
        final DirectBuffer srcMetaDataBuffer,
        final int srcMetaDataOffset,
        final int srcMetaDataLength)
    {
        isSlow = SlowStatus.SLOW == slowStatus;

        this.metaDataStatus = metaDataStatus;
        this.metaDataBuffer.wrap(srcMetaDataBuffer, srcMetaDataOffset, srcMetaDataLength);
    }
}
