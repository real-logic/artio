package uk.co.real_logic.artio.engine.logger;

import uk.co.real_logic.artio.builder.AbstractSequenceResetEncoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

class GapFillEncoder
{
    private static final int ENCODE_BUFFER_SIZE = 1024;

    private final AbstractSequenceResetEncoder sequenceResetEncoder;
    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[ENCODE_BUFFER_SIZE]);

    GapFillEncoder(final AbstractSequenceResetEncoder sequenceResetEncoder)
    {
        this.sequenceResetEncoder = sequenceResetEncoder;
        this.sequenceResetEncoder.header().possDupFlag(true);
        this.sequenceResetEncoder.gapFillFlag(true);
    }

    long encode(final int msgSeqNum, final int newSeqNo)
    {
        final SessionHeaderEncoder respHeader = sequenceResetEncoder.header();
        respHeader.sendingTime(timestampEncoder.buffer(), timestampEncoder.encode(System.currentTimeMillis()));
        respHeader.msgSeqNum(msgSeqNum);
        sequenceResetEncoder.newSeqNo(newSeqNo);

        return sequenceResetEncoder.encode(buffer, 0);
    }

    void setupMessage(final SessionHeaderDecoder reqHeader)
    {
        final SessionHeaderEncoder respHeader = sequenceResetEncoder.header();
        respHeader.targetCompID(reqHeader.senderCompID(), reqHeader.senderCompIDLength());
        respHeader.senderCompID(reqHeader.targetCompID(), reqHeader.targetCompIDLength());
        if (reqHeader.hasSenderLocationID())
        {
            respHeader.targetLocationID(reqHeader.senderLocationID(), reqHeader.senderLocationIDLength());
        }
        if (reqHeader.hasSenderSubID())
        {
            respHeader.targetSubID(reqHeader.senderSubID(), reqHeader.senderSubIDLength());
        }
        if (reqHeader.hasTargetLocationID())
        {
            respHeader.senderLocationID(reqHeader.targetLocationID(), reqHeader.targetLocationIDLength());
        }
        if (reqHeader.hasTargetSubID())
        {
            respHeader.senderSubID(reqHeader.targetSubID(), reqHeader.targetSubIDLength());
        }
    }

    MutableAsciiBuffer buffer()
    {
        return buffer;
    }
}
