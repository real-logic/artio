package uk.co.real_logic.artio.dictionary.generation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

final class OptionalSessionFields
{
    static final Map<String, List<String>> ENCODER_OPTIONAL_SESSION_FIELDS = new HashMap<>();
    static final Map<String, List<String>> DECODER_OPTIONAL_SESSION_FIELDS = new HashMap<>();

    static
    {
        final List<String> logonFields = asList("Username", "Password");
        ENCODER_OPTIONAL_SESSION_FIELDS.put("LogonEncoder", logonFields);
        DECODER_OPTIONAL_SESSION_FIELDS.put("LogonDecoder", logonFields);

        final List<String> rejectFields = asList("RefMsgType");
        ENCODER_OPTIONAL_SESSION_FIELDS.put("RejectEncoder", rejectFields);
    }
}
