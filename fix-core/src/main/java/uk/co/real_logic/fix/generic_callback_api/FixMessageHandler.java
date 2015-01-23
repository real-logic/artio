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
package uk.co.real_logic.fix.generic_callback_api;

import uk.co.real_logic.fix.util.MutableString;

public interface FixMessageHandler
{

    void onStartMessage();

    void onStringField(int tag, MutableString value);

    void onIntField(int tag, int value);

    void onGroup(int tag, int numberOfElements);

    void onEndMessage();

}
