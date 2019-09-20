package uk.co.real_logic.artio.engine;

import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.decoder.HeaderDecoder;

public class HeaderSetup
{
    public static void setup(final HeaderDecoder reqHeader, final SessionHeaderEncoder respHeader)
    {
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
}
