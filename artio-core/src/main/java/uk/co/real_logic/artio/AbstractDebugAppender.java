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
package uk.co.real_logic.artio;

/**
 * API to enable integrate the DebugLogger API into log4j / slf4j. Extend this class in
 * order to implement custom logging backends.
 */
public abstract class AbstractDebugAppender
{
    /**
     * An instane of this thread local appender is created for each thread within the system.
     *
     * You can rely on calls to its log() method only being on a given thread and allocate and use state in an
     * uncontended and unsynchronized manner.
     */
    public abstract static class ThreadLocalAppender
    {
        public abstract void log(LogTag logTag, StringBuilder stringBuilder);
    }

    /**
     * Create an instance of {@link ThreadLocalAppender}
     *
     * @return the new thread local appender.
     */
    public abstract ThreadLocalAppender makeLocalAppender();
}
