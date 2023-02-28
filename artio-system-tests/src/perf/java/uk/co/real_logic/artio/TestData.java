/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;

@SuppressWarnings("LineLength")
public final class TestData
{
    public static final UnsafeBuffer NEW_ORDER_SINGLE = new UnsafeBuffer(
        ("8=FIX.4.2\0019=145\00135=D\00134=4\00149=ABC_DEFG01\001" +
        "52=20090323-15:40:29\00156=CCG\001115=XYZ\00111=NF 0542/03232009\00154=1\00138=100\00155=CVS\00140=1" +
        "\00159=0\00147=A\00160=20090323-15:40:29\00121=1\001207=N\00110=194\001").getBytes(StandardCharsets.US_ASCII));

    public static final UnsafeBuffer LOGON = new UnsafeBuffer(
        ("8=FIX.4.4\0019=0103\00135=A\00149=ABC_DEFG01\00156=CCG\00134=10\001" +
        "52=20150514-15:57:31.336\00198=0\001108=10\001383=512\001553=username" +
        "\001554=password\00110=243\001").getBytes(StandardCharsets.US_ASCII));
}
