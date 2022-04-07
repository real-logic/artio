/*
 * Copyright 2022 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.engine.framer.FramerContext;

public class ReplayEvictionHandler
{
    private final ErrorHandler errorHandler;
    private ReplayQuery replayQuery;
    private ReplayQuery framerReplayQuery;
    private FramerContext framerContext;

    public ReplayEvictionHandler(final ErrorHandler errorHandler)
    {
        this.errorHandler = errorHandler;
    }

    public void onReset(final long fixSessionId)
    {
        if (replayQuery != null)
        {
            replayQuery.onReset(fixSessionId);
        }

        if (framerReplayQuery != null)
        {
            if (framerContext == null)
            {
                errorHandler.onError(new NullPointerException("framerContext in ReplayEvictionHandler"));
                return;
            }

            framerContext.resetOutboundReplayQuery(fixSessionId);
        }
    }

    public void replayQuery(final ReplayQuery replayQuery)
    {
        if (this.replayQuery != null)
        {
            errorHandler.onError(new IllegalStateException("duplicate replay query eviction handling"));
            return;
        }
        this.replayQuery = replayQuery;
    }

    public void framerReplayQuery(final ReplayQuery framerReplayQuery)
    {
        this.framerReplayQuery = framerReplayQuery;
    }

    public void framerContext(final FramerContext framerContext)
    {
        this.framerContext = framerContext;
    }
}
