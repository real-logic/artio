package uk.co.real_logic.artio.binary_entrypoint;

import org.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.artio.engine.framer.FixPContexts;
import uk.co.real_logic.artio.fixp.AbstractFixPStorage;
import uk.co.real_logic.artio.fixp.FixPContext;
import uk.co.real_logic.artio.fixp.FixPKey;

import java.nio.ByteOrder;

import static org.agrona.BitUtil.SIZE_OF_LONG;

public class BinaryEntryPointStorage extends AbstractFixPStorage
{
    private static final int SESSION_ID_OFFSET = 0;
    private static final int SESSION_ID_LENGTH = SIZE_OF_LONG;
    private static final int SESSION_VER_ID_OFFSET = SESSION_ID_OFFSET + SESSION_ID_LENGTH;
    private static final int SESSION_VER_ID_LENGTH = SIZE_OF_LONG;
    private static final int TIMESTAMP_OFFSET = SESSION_VER_ID_OFFSET + SESSION_VER_ID_LENGTH;
    private static final int TIMESTAMP_LENGTH = SIZE_OF_LONG;
    private static final int ENTERING_FIRM_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    private static final int ENTERING_FIRM_LENGTH = SIZE_OF_LONG;
    private static final int ENTRY_LENGTH = ENTERING_FIRM_OFFSET + ENTERING_FIRM_LENGTH;

    public BinaryEntryPointStorage()
    {
        super();
    }

    public BinaryEntryPointContext newInitiatorContext(
        final FixPKey key, final int offset, final FixPContexts contexts)
    {
        throw new UnsupportedOperationException();
    }

    public BinaryEntryPointContext loadContext(
        final AtomicBuffer buffer, final int offset, final int fileVersion, final FixPContexts contexts)
    {
        final long sessionId = buffer.getLong(offset + SESSION_ID_OFFSET);
        final long sessionVerId = buffer.getLong(offset + SESSION_VER_ID_OFFSET);
        final long timestamp = buffer.getLong(offset + TIMESTAMP_OFFSET);
        final long enteringFirm = buffer.getLong(offset + ENTERING_FIRM_OFFSET);

        final BinaryEntryPointContext context = new BinaryEntryPointContext(
            sessionId, sessionVerId, timestamp, enteringFirm, false);
        context.offset(offset);
        return context;
    }

    public int saveContext(
        final FixPContext fixPContext, final AtomicBuffer buffer, final int offset, final int fileVersion)
    {
        final BinaryEntryPointContext context = (BinaryEntryPointContext)fixPContext;
        context.offset(offset);

        buffer.putLong(offset + SESSION_ID_OFFSET, context.sessionID(), ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(offset + SESSION_VER_ID_OFFSET, context.sessionVerID(), ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(offset + TIMESTAMP_OFFSET, context.requestTimestamp(), ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(offset + ENTERING_FIRM_OFFSET, context.enteringFirm(), ByteOrder.LITTLE_ENDIAN);

        return ENTRY_LENGTH;
    }

    public void updateContext(
        final FixPContext fixPContext, final AtomicBuffer buffer)
    {
        final BinaryEntryPointContext context = (BinaryEntryPointContext)fixPContext;
        final int offset = context.offset();

        buffer.putLong(offset + SESSION_VER_ID_OFFSET, context.sessionVerID(), ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(offset + TIMESTAMP_OFFSET, context.requestTimestamp(), ByteOrder.LITTLE_ENDIAN);
    }
}
