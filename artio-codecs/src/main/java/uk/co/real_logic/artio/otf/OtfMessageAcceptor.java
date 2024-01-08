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
package uk.co.real_logic.artio.otf;

import uk.co.real_logic.artio.ValidationError;
import uk.co.real_logic.artio.fields.AsciiFieldFlyweight;
import uk.co.real_logic.artio.util.AsciiBuffer;

public interface OtfMessageAcceptor
{
    MessageControl onNext();

    MessageControl onComplete();

    MessageControl onField(int tag, AsciiBuffer buffer, int offset, int length);

    /**
     * Called at the beginning of a repeating group.
     *
     * @param tag the tag number of the field representing the number of elements, eg NoAllocs
     * @param numInGroup the total number of elements in the group
     * @return whether to continue parsing or stop
     */
    MessageControl onGroupHeader(int tag, int numInGroup);

    /**
     * Called at the beginning of each group entry.
     *
     * @param tag the tag number of the field representing the number of elements, eg NoAllocs
     * @param numInGroup the total number of elements in the group
     * @param index the index of the element within the group
     * @return whether to continue parsing or stop
     */
    MessageControl onGroupBegin(int tag, int numInGroup, int index);

    /**
     * Called at the end of each group entry
     *
     * @param tag the tag number of the field representing the number of elements, eg NoAllocs
     * @param numInGroup the total number of elements in the group
     * @param index the index of the element within the group
     * @return whether to continue parsing or stop
     */
    MessageControl onGroupEnd(int tag, int numInGroup, int index);

    boolean onError(ValidationError error, long messageType, int tagNumber, AsciiFieldFlyweight value);
}
