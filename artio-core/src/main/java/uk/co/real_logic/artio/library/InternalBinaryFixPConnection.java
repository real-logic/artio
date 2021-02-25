/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import uk.co.real_logic.artio.messages.DisconnectReason;

public abstract class InternalBinaryFixPConnection implements BinaryFixPConnection
{
    // -----------------------------------------------
    // Internal Methods below, not part of the public API
    // -----------------------------------------------

    protected abstract int poll(long timeInMs);

    protected abstract void onReplayComplete();

    protected abstract void fullyUnbind();

    protected abstract void unbindState(DisconnectReason reason);
}
