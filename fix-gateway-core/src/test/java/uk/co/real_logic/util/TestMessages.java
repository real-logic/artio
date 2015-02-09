/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.util;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class TestMessages
{
    // BUY 100 CVS MKT DAY
    public static final byte[] EG_MESSAGE = toAscii("8=FIX.4.2\0019=145\00135=D\00134=4\00149=ABC_DEFG01\001" +
            "52=20090323-15:40:29\00156=CCG\001115=XYZ\00111=NF 0542/03232009\00154=1\00138=100\00155=CVS\00140=1" +
            "\00159=0\00147=A\00160=20090323-15:40:29\00121=1\001207=N\00110=194\001");

    public static final int MSG_LEN = EG_MESSAGE.length;

    public static final byte[] INVALID_CHECKSUM_MSG = toAscii("8=FIX.4.2\0019=145\00135=D\00134=4\00149=ABC_DEFG01\001" +
            "52=20090323-15:40:29\00156=CCG\001115=XYZ\00111=NF 0542/03232009\00155=CVS\00140=1\00159=0\00147=A" +
            "60=20090323-15:40:29\00121=1\001207=N\00110=155\001");

    public static final int INVALID_CHECKSUM_LEN = INVALID_CHECKSUM_MSG.length;

    public static final byte[] INVALID_MESSAGE = toAscii("8=FIX.4.2\0019=145\00135=D\00134=4\00149=ABC_DEFG01\001" +
            "52=\\\\\20156=CCG\001115=XYZ\00111=NF 0542/03232009\001\001\001\001\001\00154=1\00138=55140=" +
            "\00159=0\00147=A\00160=20090323-15:40:29\00121=1\001207=N\00110=194\001");

    public static final int INVALID_LEN = INVALID_MESSAGE.length;

    private static byte[] toAscii(String str)
    {
        return str.getBytes(US_ASCII);
    }

}
