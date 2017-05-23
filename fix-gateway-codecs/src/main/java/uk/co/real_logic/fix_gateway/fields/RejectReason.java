/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.fields;

/**
 * SessionRejectReason which is common across both FIX 4.2 and 4.4 versions to be used by session parsing logic.
 */
public enum RejectReason
{
    INVALID_TAG_NUMBER(0),
    REQUIRED_TAG_MISSING(1),
    TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE(2),
    UNDEFINED_TAG(3),
    TAG_SPECIFIED_WITHOUT_A_VALUE(4),
    VALUE_IS_INCORRECT(5),
    INCORRECT_DATA_FORMAT_FOR_VALUE(6),
    DECRYPTION_PROBLEM(7),
    SIGNATURE_PROBLEM(8),
    COMPID_PROBLEM(9),
    SENDINGTIME_ACCURACY_PROBLEM(10),
    INVALID_MSGTYPE(11),
    OTHER(99);

    private final int representation;

    RejectReason(final int representation)
    {
        this.representation = representation;
    }

    public final int representation()
    {
        return representation;
    }

    public static RejectReason decode(final int representation)
    {
        switch (representation)
        {
            case 0: return INVALID_TAG_NUMBER;
            case 1: return REQUIRED_TAG_MISSING;
            case 2: return TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE;
            case 3: return UNDEFINED_TAG;
            case 4: return TAG_SPECIFIED_WITHOUT_A_VALUE;
            case 5: return VALUE_IS_INCORRECT;
            case 6: return INCORRECT_DATA_FORMAT_FOR_VALUE;
            case 7: return DECRYPTION_PROBLEM;
            case 8: return SIGNATURE_PROBLEM;
            case 9: return COMPID_PROBLEM;
            case 10: return SENDINGTIME_ACCURACY_PROBLEM;
            case 11: return INVALID_MSGTYPE;
            case 99: return OTHER;
            default: throw new IllegalArgumentException("Unknown: " + representation);
        }
    }
}
