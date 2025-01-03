/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.engine;

import org.agrona.collections.Long2LongHashMap;

public class CompletionPosition
{
    public static final int MISSING_VALUE = -1;

    private Long2LongHashMap positions;
    private volatile boolean complete = false;

    public Long2LongHashMap positions()
    {
        return positions;
    }

    public void completeDuringStartup()
    {
        complete = true;
    }

    public void complete(final Long2LongHashMap positions)
    {
        this.positions = positions;
        complete = true;
    }

    public boolean hasCompleted()
    {
        return complete;
    }

    public boolean wasStartupComplete()
    {
        return positions == null;
    }
}
