package uk.co.real_logic.artio.engine.logger;

import uk.co.real_logic.artio.builder.AbstractSequenceResetEncoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.engine.HeaderSetup;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

class GapFillEncoder
{
    private static final int ENCODE_BUFFER_SIZE = 1024;

    private final AbstractSequenceResetEncoder sequenceResetEncoder;
    private final UtcTimestampEncoder timestampEncoder;
    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[ENCODE_BUFFER_SIZE]);

    GapFillEncoder(final AbstractSequenceResetEncoder sequenceResetEncoder, final UtcTimestampEncoder timestampEncoder)
    {
        this.sequenceResetEncoder = sequenceResetEncoder;
        this.timestampEncoder = timestampEncoder;
        this.sequenceResetEncoder.header().possDupFlag(true);
        this.sequenceResetEncoder.gapFillFlag(true);
    }

    long encode(final int msgSeqNum, final int newSeqNo)
    {
        final SessionHeaderEncoder respHeader = sequenceResetEncoder.header();
        respHeader.sendingTime(timestampEncoder.buffer(),
            timestampEncoder.encodeFrom(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
        respHeader.msgSeqNum(msgSeqNum);
        sequenceResetEncoder.newSeqNo(newSeqNo);

        return sequenceResetEncoder.encode(buffer, 0);
    }

    void setupMessage(final SessionHeaderDecoder requestHeader)
    {
        HeaderSetup.setup(requestHeader, sequenceResetEncoder.header());
    }

    MutableAsciiBuffer buffer()
    {
        return buffer;
    }
}
