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
package uk.co.real_logic.artio.ilink;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.library.NotAppliedResponse;

// NB: This is an experimental API and is subject to change or potentially removal.
public interface ILink3SessionHandler
{
    void onMessage(
        DirectBuffer buffer,
        int offset,
        int length);

    void onNotApplied(long fromSequenceNumber, long msgCount, NotAppliedResponse response);

    void onRetransmitReject(String reason, long requestTimestamp, int errorCodes);
}
