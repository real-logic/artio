package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.messages.FixMessageDecoder;

public final class MessageTypeExtractor
{
    private static final int LAST_SCHEMA_VERSION_WITH_INT_MESSAGE_TYPES = 2;

    public static long getMessageType(final FixMessageDecoder messageDecoder)
    {
        return messageDecoder.sbeSchemaVersion() > LAST_SCHEMA_VERSION_WITH_INT_MESSAGE_TYPES ?
                messageDecoder.messageType() : messageDecoder.deprecatedMessageType();
    }
}
