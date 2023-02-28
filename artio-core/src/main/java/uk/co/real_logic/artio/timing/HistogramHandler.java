/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.timing;

import org.HdrHistogram.Histogram;

/**
 * Callback interface in order to hook the logging of histograms to a file.
 *
 * Each operation has a timer associated with it that has a name and a unique id. You
 * receive callbacks for all of the timers.
 */
public interface HistogramHandler extends AutoCloseable
{
    /**
     * Associate an id of the operation being measured with the name of the operation.
     *
     * @param id the unique id of the operation being measured
     * @param name the human readable name of of the operation being measured
     */
    void identifyTimer(int id, String name);

    /**
     * Callback after all the timers have been identified.
     */
    void onEndTimerIdentification();

    /**
     * Receive a new histogram of timings for an operation.
     *
     * @param id the unique id of the operation being measured
     * @param histogram the histogram of timings for the operation being measured
     */
    void onTimerUpdate(int id, Histogram histogram);

    /**
     * Callback before after all onTimerUpdate calls happen.
     * @param currentTimeInMs at which to being recording.
     */
    void onBeginTimerUpdate(long currentTimeInMs);

    /**
     * Callback before after all onTimerUpdate calls happen.
     */
    void onEndTimerUpdate();

}
