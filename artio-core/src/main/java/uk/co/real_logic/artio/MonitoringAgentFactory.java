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
package uk.co.real_logic.artio;

import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.ErrorConsumer;

public interface MonitoringAgentFactory
{
    /**
     * Use a factory that prints out distinct error messages on standard error from a saved
     * {@link DistinctErrorLog}. This assumes that
     * {@link CommonConfiguration#errorHandlerFactory(ErrorHandlerFactory)} is set to
     * {@link ErrorHandlerFactory#saveDistinctErrors()}.
     *
     * @return the factory
     */
    static MonitoringAgentFactory printDistinctErrors()
    {
        return consumeDistinctErrors(ErrorPrinter.PRINTING_ERROR_CONSUMER);
    }

    /**
     * Use a factory that takes a custom error consumer from a saved
     * {@link DistinctErrorLog}. This assumes that
     * {@link CommonConfiguration#errorHandlerFactory(ErrorHandlerFactory)} is set to
     * {@link ErrorHandlerFactory#saveDistinctErrors()}.
     *
     * @param errorConsumer the callback that receives errors from a distinct error log
     * @return the factory
     */
    static MonitoringAgentFactory consumeDistinctErrors(
        final ErrorConsumer errorConsumer)
    {
        return (errorBuffer, agentNamePrefix, aeronArchive) -> new ErrorPrinter(
            errorBuffer,
            agentNamePrefix,
            System.currentTimeMillis(),
            aeronArchive,
            errorConsumer,
            new SystemEpochClock());
    }

    /**
     * Don't print out errors from the {@link DistinctErrorLog}.
     *
     * @return the factory
     */
    static MonitoringAgentFactory none()
    {
        return null;
    }

    MonitoringAgent make(
        AtomicBuffer errorBuffer,
        String agentNamePrefix,
        AeronArchive aeronArchive);
}
