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
package uk.co.real_logic.artio.fixp;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.sbe.ir.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.co.real_logic.sbe.ir.Signal.BEGIN_FIELD;
import static uk.co.real_logic.sbe.ir.Signal.VALID_VALUE;
import static uk.co.real_logic.sbe.ir.Token.VARIABLE_LENGTH;

/**
 * Generic way to lookup client order id fields that are used for business reject ref ids for FIXP protocols. It needs
 * to take the Ir object in order to be initiated. This can be looked up using
 * {@link AbstractFixPOffsets#loadSbeIr(Class, String)} or {@link IrDecoder#decode()};
 *
 * Call the <code>search</code> method with a buffer in order to find the reject ref id for a given FIXP message.
 *
 * Single threaded.
 */
public class FixPRejectRefIdExtractor
{
    public static final int MISSING_OFFSET = -1;
    public static final String MESSAGE_TYPE = "MessageType";
    public static final String CLORDID = "clordid";

    private final int templateIdOffset;
    private final int headerLength;
    private final Int2IntHashMap templateIdToLength = new Int2IntHashMap(MISSING_OFFSET);
    private final Int2IntHashMap templateIdToOffset = new Int2IntHashMap(MISSING_OFFSET);
    private final Long2LongHashMap templateIdToMessageType = new Long2LongHashMap(MISSING_OFFSET);

    private int length;
    private int templateId;
    private int offset;
    private long messageType;

    public FixPRejectRefIdExtractor(final Ir ir)
    {
        final Map<String, Long> messageTypeToValue = new HashMap<>();
        final List<Token> messageType = ir.getType(MESSAGE_TYPE);
        if (messageType != null)
        {
            messageType
                .stream()
                .filter(token -> token.signal() == VALID_VALUE)
                .forEach(token ->
                {
                    final Encoding encoding = token.encoding();
                    final long value = encoding.constValue().longValue();
                    messageTypeToValue.put(MESSAGE_TYPE + "." + token.name(), value);
                });
        }

        ir.messages().forEach(messageTokens ->
        {
            final int templateId = AbstractFixPOffsets.templateId(messageTokens);

            messageTokens.stream()
                .filter(token -> nameIgnoreCase(token, CLORDID))
                .findFirst()
                .ifPresent(token ->
                {
                    templateIdToLength.put(templateId, token.encodedLength());
                    templateIdToOffset.put(templateId, token.offset());
                });

            messageTokens.stream()
                .filter(token -> nameIgnoreCase(token, MESSAGE_TYPE) && token.signal() == BEGIN_FIELD)
                .mapToLong(token -> messageTypeToValue.get(token.encoding().constValue().toString()))
                .filter(id -> id != MISSING_OFFSET)
                .findFirst()
                .ifPresent(messageTypeValue -> templateIdToMessageType.put(templateId, messageTypeValue));
        });

        final HeaderStructure headerStructure = ir.headerStructure();

        templateIdOffset = headerStructure.tokens().stream()
            .filter(token -> token.name().equals("templateId") && isEncoding(token))
            .mapToInt(Token::offset)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Unable to find template id for FIXP protocol header"));

        headerLength = headerStructure.tokens().stream()
            .filter(this::isEncoding)
            .mapToInt(token ->
            {
                final int encodedLength = token.encodedLength();
                if (encodedLength == VARIABLE_LENGTH)
                {
                    throw new IllegalStateException("FIXP protocol message header with variable length tokens");
                }
                return encodedLength;
            }).sum();
    }

    private boolean nameIgnoreCase(final Token token, final String clordid)
    {
        return token.name().equalsIgnoreCase(clordid);
    }

    private boolean isEncoding(final Token token)
    {
        return token.signal() == Signal.ENCODING;
    }

    /**
     * Call to search a buffer for a given client order id if the message in question exists. The accessor
     * fields below are populated and can be used to retrieve the reject ref id, template id and message type of the
     * message in question.
     *
     * @param buffer a buffer containing a message
     * @param offset an offset into the buffer at the start of the SOFH.
     * @return true if a reject ref id could be found, false otherwise.
     */
    public boolean search(final DirectBuffer buffer, final int offset)
    {
        final int headerOffset = offset + SimpleOpenFramingHeader.SOFH_LENGTH;

        templateId = templateId(buffer, headerOffset);
        messageType = templateIdToMessageType.get(templateId);
        length = templateIdToLength.get(templateId);
        final int fieldOffset = templateIdToOffset.get(templateId);

        if (fieldOffset == MISSING_OFFSET)
        {
            this.offset = MISSING_OFFSET;
            return false;
        }
        else
        {
            this.offset = headerOffset + headerLength + fieldOffset;
            return true;
        }
    }

    public int templateId(final DirectBuffer buffer, final int headerOffset)
    {
        return (buffer.getShort(headerOffset + templateIdOffset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF);
    }

    /**
     * Get the template id of the message in question. If it doesn't exist then returns {@link #MISSING_OFFSET}.
     * You need to call search in order for this field to be populated.
     *
     * @return the template id of the message in question.
     */
    public int templateId()
    {
        return templateId;
    }

    /**
     * Get the offset of the client order id within the buffer passed to {@link #search(DirectBuffer, int)}.
     * If it doesn't exist then returns {@link #MISSING_OFFSET}.
     * You need to call search in order for this field to be populated.
     *
     * @return the offset of the client order id.
     */
    public int offset()
    {
        return offset;
    }

    /**
     * Get the length of the client order id within the buffer passed to {@link #search(DirectBuffer, int)}.
     * If it doesn't exist then returns {@link #MISSING_OFFSET}.
     * You need to call search in order for this field to be populated.
     *
     * @return the length of the client order id.
     */
    public int length()
    {
        return length;
    }

    /**
     * Get the message type of the message in question. If it doesn't exist then returns {@link #MISSING_OFFSET}.
     * You need to call search in order for this field to be populated.
     *
     * @return the message type of the message in question.
     */
    public long messageType()
    {
        return messageType;
    }
}
