package uk.co.real_logic.artio.engine.logger;

import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.SequenceResetEncoder;
import uk.co.real_logic.artio.decoder.HeaderDecoder;
import uk.co.real_logic.artio.engine.HeaderSetup;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

class GapFillEncoder
{
    private static final int ENCODE_BUFFER_SIZE = 1024;

    private final SequenceResetEncoder sequenceResetEncoder = new SequenceResetEncoder();
    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[ENCODE_BUFFER_SIZE]);

    GapFillEncoder()
    {
        sequenceResetEncoder.header().possDupFlag(true);
        sequenceResetEncoder.gapFillFlag(true);
    }

    long encode(final int msgSeqNum, final int newSeqNo)
    {
        final HeaderEncoder respHeader = sequenceResetEncoder.header();
        respHeader.sendingTime(timestampEncoder.buffer(), timestampEncoder.encode(System.currentTimeMillis()));
        respHeader.msgSeqNum(msgSeqNum);
        sequenceResetEncoder.newSeqNo(newSeqNo);

        return sequenceResetEncoder.encode(buffer, 0);
    }

    void setupMessage(final HeaderDecoder requestHeader)
    {
        HeaderSetup.setup(requestHeader, sequenceResetEncoder.header());
    }

    MutableAsciiBuffer buffer()
    {
        return buffer;
    }
}
