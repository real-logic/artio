/*
 * Copyright 2015-2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHbuilder WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.sbe_util;

import org.agrona.DirectBuffer;
import uk.co.real_logic.sbe.PrimitiveValue;
import uk.co.real_logic.sbe.ir.Encoding;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.otf.Types;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class DumpingTokenListener implements TokenListener
{
    public static final String INDENT = "    ";
    private final StringBuilder builder = new StringBuilder();
    private final StringBuilder prefix = new StringBuilder();

    public void onBeginMessage(final Token token)
    {
        prefix.append(INDENT);
        builder
            .append("{\n")
            .append(prefix)
            .append("Template: '")
            .append(token.name())
            .append('\'');
    }

    public void onEndMessage(final Token token)
    {
        final int length = prefix.length();
        if (length > 0)
        {
            prefix.setLength(length - INDENT.length());
        }
        builder.append("\n}");
    }

    public void onEncoding(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int index,
        final Token typeToken,
        final int actingVersion)
    {
        final CharSequence value = readEncodingAsString(buffer, index, typeToken, actingVersion);

        appendRecord(fieldToken, value);
    }

    private void appendRecord(final Token fieldToken, final CharSequence value)
    {
        appendPrefix()
            .append(fieldToken.name())
            .append(": ")
            .append(value);
    }

    private StringBuilder appendPrefix()
    {
        return builder
            .append(",\n")
            .append(prefix);
    }

    public void onEnum(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int bufferIndex,
        final List<Token> tokens,
        final int beginIndex,
        final int endIndex,
        final int actingVersion)
    {
        final Token typeToken = tokens.get(beginIndex + 1);
        final long encodedValue = readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

        String value = null;
        for (int i = beginIndex + 1; i < endIndex; i++)
        {
            if (encodedValue == tokens.get(i).encoding().constValue().longValue())
            {
                value = tokens.get(i).name();
                break;
            }
        }

        appendRecord(fieldToken, '\'' + value + '\'');
    }

    public void onBitSet(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int bufferIndex,
        final List<Token> tokens,
        final int beginIndex,
        final int endIndex,
        final int actingVersion)
    {
        final Token typeToken = tokens.get(beginIndex + 1);
        final long encodedValue = readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

        appendPrefix();
        builder.append(fieldToken.name()).append(':');

        for (int i = beginIndex + 1; i < endIndex; i++)
        {
            builder.append(' ').append(tokens.get(i).name()).append('=');

            final long bitPosition = tokens.get(i).encoding().constValue().longValue();
            final boolean flag = (encodedValue & (1L << bitPosition)) != 0;

            builder.append(Boolean.toString(flag));
        }

        builder.append('\n');
    }

    public void onBeginComposite(
        final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex)
    {
        //namedScope.push(fieldToken.name() + ".");
    }

    public void onEndComposite(
        final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex)
    {
    }

    public void onGroupHeader(final Token token, final int numInGroup)
    {
        /*printScope();
        builder.append(token.name())
            .append(" Group Header : numInGroup=")
            .append(Integer.toString(numInGroup))
            .append('\n');*/
    }

    public void onBeginGroup(final Token token, final int groupIndex, final int numInGroup)
    {
    }

    public void onEndGroup(final Token token, final int groupIndex, final int numInGroup)
    {
    }

    public void onVarData(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int bufferIndex,
        final int length,
        final Token typeToken)
    {
        final String value;
        try
        {
            final byte[] tempBuffer = new byte[length];
            buffer.getBytes(bufferIndex, tempBuffer, 0, length);
            value = new String(tempBuffer, 0, length, typeToken.encoding().characterEncoding());
        }
        catch (final UnsupportedEncodingException ex)
        {
            ex.printStackTrace();
            return;
        }

        appendRecord(fieldToken, '\'' + value + '\'');
    }

    private static CharSequence readEncodingAsString(
        final DirectBuffer buffer, final int index, final Token typeToken, final int actingVersion)
    {
        final PrimitiveValue constOrNotPresentValue = constOrNotPresentValue(typeToken, actingVersion);
        if (null != constOrNotPresentValue)
        {
            return constOrNotPresentValue.toString();
        }

        final StringBuilder sb = new StringBuilder();
        final Encoding encoding = typeToken.encoding();
        final int elementSize = encoding.primitiveType().size();

        for (int i = 0, size = typeToken.arrayLength(); i < size; i++)
        {
            Types.appendAsString(sb, buffer, index + (i * elementSize), encoding);
            sb.append(", ");
        }

        sb.setLength(sb.length() - 2);

        return sb;
    }

    private static long readEncodingAsLong(
        final DirectBuffer buffer, final int bufferIndex, final Token typeToken, final int actingVersion)
    {
        final PrimitiveValue constOrNotPresentValue = constOrNotPresentValue(typeToken, actingVersion);
        if (null != constOrNotPresentValue)
        {
            return constOrNotPresentValue.longValue();
        }

        return Types.getLong(buffer, bufferIndex, typeToken.encoding());
    }

    private static PrimitiveValue constOrNotPresentValue(final Token token, final int actingVersion)
    {
        if (token.isConstantEncoding())
        {
            return token.encoding().constValue();
        }
        else if (token.isOptionalEncoding() && actingVersion < token.version())
        {
            return token.encoding().applicableNullValue();
        }

        return null;
    }

    public String toString()
    {
        return builder.toString();
    }
}
