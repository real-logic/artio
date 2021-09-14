/*
 * Copyright 2021 Monotonic Ltd.
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

import org.agrona.collections.LongArrayList;
import uk.co.real_logic.artio.engine.MappedFile;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.engine.framer.FixContexts;
import uk.co.real_logic.artio.session.SessionIdStrategy;

import java.io.File;
import java.util.List;
import java.util.function.Predicate;

import static org.agrona.collections.LongArrayList.DEFAULT_NULL_VALUE;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_SESSION_ID_FILE;

class ArchiveScanPredicateOptimizer
{
    private final String logFileDir;

    ArchiveScanPredicateOptimizer(final String logFileDir)
    {
        this.logFileDir = logFileDir;
    }

    void optimizeSessionIds(final FixMessagePredicate queryPredicate)
    {
        final Predicate<SessionInfo> sessionPlan;
        try
        {
            sessionPlan = ArchiveScanPlanner.extractSessionPlan(queryPredicate);
        }
        catch (final IllegalArgumentException e)
        {
            e.getMessage();
            return;
        }

        if (sessionPlan == null)
        {
            return;
        }

        System.out.println("sessionPlan = " + sessionPlan);

        final File sessionIdFile = new File(logFileDir + File.separator + DEFAULT_SESSION_ID_FILE);
        if (!sessionIdFile.exists())
        {
            return;
        }

//        sessionIdFile.lastModified();

        final FixContexts contexts = new FixContexts(
            MappedFile.map(sessionIdFile, 0),
            SessionIdStrategy.senderAndTarget(),
            0,
            Throwable::printStackTrace);

        final List<SessionInfo> sessionInfos = contexts.allSessions();
        final int n = sessionInfos.size();
        final LongArrayList sessionIds = new LongArrayList(n, DEFAULT_NULL_VALUE);
        for (int i = 0; i < n; i++)
        {
            final SessionInfo sessionInfo = sessionInfos.get(i);
            if (sessionPlan.test(sessionInfo))
            {
                sessionIds.add(sessionInfo.sessionId());
            }
        }
    }
}
