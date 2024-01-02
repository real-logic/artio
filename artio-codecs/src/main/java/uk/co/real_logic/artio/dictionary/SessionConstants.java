/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio.dictionary;

public final class SessionConstants
{
    public static final byte START_OF_HEADER = 0x01;

    public static final int FIX4_HEADER_LENGTH = "8=FIX.4.2 ".length();
    public static final int FIXT_HEADER_LENGTH = "8=FIXT.1.1 ".length();

    // header and message length tag
    public static final int MIN_MESSAGE_SIZE = FIXT_HEADER_LENGTH + 6;

    // Message Types
    public static final int BEGIN_SEQ_NO = 7;
    public static final int END_SEQ_NO = 16;
    public static final int BODY_LENGTH = 9;
    public static final int CHECKSUM = 10;
    public static final int MSG_SEQ_NO = 34;
    public static final int MESSAGE_TYPE = 35;
    public static final int NEW_SEQ_NO = 36;
    public static final int POSS_DUP_FLAG = 43;
    public static final int SENDER_COMP_ID = 49;
    public static final int SENDER_SUB_ID = 50;
    public static final int SENDING_TIME = 52;
    public static final int TARGET_COMP_ID = 56;
    public static final int TARGET_SUB_ID = 57;
    public static final int ORIG_SENDING_TIME = 122;
    public static final int SENDER_LOCATION_ID = 142;
    public static final int TARGET_LOCATION_ID = 143;
    public static final int PASSWORD = 554;
    public static final int NEW_PASSWORD = 925;


    public static final long LOGON_MESSAGE_TYPE = 65L;
    public static final long HEARTBEAT_MESSAGE_TYPE = 48L;
    public static final long TEST_REQUEST_MESSAGE_TYPE = 49L;
    public static final long RESEND_REQUEST_MESSAGE_TYPE = 50L;
    public static final long REJECT_MESSAGE_TYPE = 51L;
    public static final long SEQUENCE_RESET_MESSAGE_TYPE = 52L;
    public static final long LOGOUT_MESSAGE_TYPE = 53L;
    public static final long BUSINESS_MESSAGE_REJECT_MESSAGE_TYPE = 106L;

    public static final int USER_REQUEST_MESSAGE_TYPE = 17730;

    public static final String LOGON_MESSAGE_TYPE_STR = "A";
    public static final String HEARTBEAT_MESSAGE_TYPE_STR = "0";
    public static final String TEST_REQUEST_MESSAGE_TYPE_STR = "1";
    public static final String RESEND_REQUEST_MESSAGE_TYPE_STR = "2";
    public static final String REJECT_MESSAGE_TYPE_STR = "3";
    public static final String SEQUENCE_RESET_TYPE_STR = "4";
    public static final String LOGOUT_MESSAGE_TYPE_STR = "5";

    public static final byte SEQUENCE_RESET_TYPE_BYTE = (byte)'4';

    public static final char[] LOGON_MESSAGE_TYPE_CHARS = LOGON_MESSAGE_TYPE_STR.toCharArray();
    public static final char[] HEARTBEAT_MESSAGE_TYPE_CHARS = HEARTBEAT_MESSAGE_TYPE_STR.toCharArray();
    public static final char[] TEST_REQUEST_MESSAGE_TYPE_CHARS = TEST_REQUEST_MESSAGE_TYPE_STR.toCharArray();
    public static final char[] REJECT_MESSAGE_TYPE_CHARS = REJECT_MESSAGE_TYPE_STR.toCharArray();
    public static final char[] SEQUENCE_RESET_MESSAGE_TYPE_CHARS = SEQUENCE_RESET_TYPE_STR.toCharArray();
    public static final char[] RESEND_REQUEST_MESSAGE_TYPE_CHARS = RESEND_REQUEST_MESSAGE_TYPE_STR.toCharArray();

    public static final int INCORRECT_DATA_FORMAT_FOR_VALUE = 6;

}
