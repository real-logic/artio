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
package uk.co.real_logic.fix_gateway;

import org.openjdk.jmh.annotations.CompilerControl;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;

import static org.openjdk.jmh.annotations.CompilerControl.Mode.DONT_INLINE;

public final class OtfNoBlackHoleAcceptor implements OtfMessageAcceptor
{
    public void onNext()
    {

    }

    @CompilerControl(DONT_INLINE)
    public void onField(final int tag, final AsciiBuffer buffer, final int offset, final int length)
    {
    }

    @CompilerControl(DONT_INLINE)
    public void onGroupHeader(final int tag, final int numInGroup)
    {
    }

    @CompilerControl(DONT_INLINE)
    public void onGroupBegin(final int tag, final int numInGroup, final int index)
    {
    }

    @CompilerControl(DONT_INLINE)
    public void onGroupEnd(final int tag, final int numInGroup, final int index)
    {
    }

    @CompilerControl(DONT_INLINE)
    public void onComplete()
    {
    }

    @CompilerControl(DONT_INLINE)
    public boolean onError(final ValidationError error,
                           final int messageType,
                           final int tagNumber,
                           final AsciiFieldFlyweight value)
    {
        return false;
    }
}
