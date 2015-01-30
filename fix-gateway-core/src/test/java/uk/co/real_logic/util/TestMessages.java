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
    public static final byte[] EG_MESSAGE = ("8=FIX.4.2 9=145 35=D 34=4 49=ABC_DEFG01 52=20090323-15:40:29 " +
            "56=CCG 115=XYZ 11=NF 0542/03232009 54=1 38=100 55=CVS 40=1 59=0 47=A" +
            "60=20090323-15:40:29 21=1 207=N 10=139 ").replace(' ', '\1').getBytes(US_ASCII);

    public static final int MSG_LEN = EG_MESSAGE.length;


}
