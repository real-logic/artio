/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio.system_tests;

public final class SessionExistsInfo
{
    private final String localCompId;
    private final String remoteCompId;
    private final long surrogateId;
    private final int logonReceivedSequenceNumber;
    private final int logonSequenceIndex;

    SessionExistsInfo(
        final String localCompId,
        final String remoteCompId,
        final long surrogateId,
        final int logonReceivedSequenceNumber,
        final int logonSequenceIndex)
    {
        this.localCompId = localCompId;
        this.remoteCompId = remoteCompId;
        this.surrogateId = surrogateId;
        this.logonReceivedSequenceNumber = logonReceivedSequenceNumber;
        this.logonSequenceIndex = logonSequenceIndex;
    }

    public String localCompId()
    {
        return localCompId;
    }

    public String remoteCompId()
    {
        return remoteCompId;
    }

    public long surrogateId()
    {
        return surrogateId;
    }

    public int logonReceivedSequenceNumber()
    {
        return logonReceivedSequenceNumber;
    }

    public int logonSequenceIndex()
    {
        return logonSequenceIndex;
    }
}
