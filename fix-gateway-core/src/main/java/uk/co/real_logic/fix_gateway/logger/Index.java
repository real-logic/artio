/*
 * Copyright 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.agrona.DirectBuffer;

/**
 * An index is a named mapping of record to a single key value.
 */
public interface Index
{
    /**
     *
     * @param buffer
     * @param offset
     * @param length
     * @return the index value for the record in question.
     */
    long extractKey(final DirectBuffer buffer, final int offset, final int length);

    default String getName()
    {
        return getClass().getSimpleName();
    }

}
