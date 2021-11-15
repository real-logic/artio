/*
 * Copyright 2021 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.artio.builder.Decoder;
import uk.co.real_logic.artio.builder.Encoder;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.dictionary.generation.CodecGenerationWrapper.dictionaryStream;
import static uk.co.real_logic.artio.dictionary.generation.CodecGenerationWrapper.setupHeader;
import static uk.co.real_logic.artio.util.Reflection.*;

/**
 * Test provides executable documentation for the "Allow Duplicate Fields" Feature.
 *
 * Duplicate fields are fields that are duplicated within an individual message. This is
 * against the FIX spec but some venues provide XML files with the duplicate fields in. This
 * test documents what happens when you try to disable the duplication checking.
 *
 * (1) If you provide duplicate fields within a FIX message - so the same field name twice
 * or a field within the message and within a component then Artio generates code that fails to compile.
 *
 * (2) If you have a duplicate field between a repeating group and a message then code is generated that
 * compiles. Whether the message can be parsed unambiguously falls into three categories.
 *   (A) The duplicate field is set in the body of the message /before/ the repeating group with the field in.
 *   It is parsed.
 *   (B) The duplicate field within the body of the message is /after/ the repeating group.
 *   This isn't unambiguously parse.
 *   (C) If only the field is set in the body of the message. It is parsed fine.
 *   (D) If only the field is set in the repeating group. It is parsed fine.
 */
public class DuplicateFieldsTest
{
    private static final CodecGenerationWrapper WRAPPER = new CodecGenerationWrapper();
    public static final String MEMBER_IDS_GROUP = "memberIDsGroup";

    private static Class<?> withGroupEncoder;
    private static Class<?> withGroupDecoder;

    private Encoder encoder;
    private Decoder decoder;

    @BeforeClass
    public static void setup() throws Exception
    {
        WRAPPER.generate(config ->
        {
            config
                .fileStreams(dictionaryStream("example_duplicate_dictionary"))
                .allowDuplicateFields(true);
        });

        withGroupEncoder = WRAPPER.compile(WRAPPER.encoder(null, "DuplicatedFieldMessage"));
        withGroupDecoder = WRAPPER.decoder("DuplicatedFieldMessage");
    }

    @Before
    public void init() throws Exception
    {
        encoder = (Encoder)withGroupEncoder.getDeclaredConstructor().newInstance();
        decoder = (Decoder)withGroupDecoder.getDeclaredConstructor().newInstance();
    }

    @Test
    public void shouldParseMessageWithDuplicateFieldOnlyUsedInBody() throws Exception
    {
        setMemberIdField();

        encodeAndDecode();

        assertMemberIdField();
    }

    @Test
    public void shouldParseMessageWithDuplicateFieldOnlyUsedInGroup() throws Exception
    {
        setMemberIdOnGroup();

        encodeAndDecode();

        assertMemberIdOnGroup();
    }

    @Test
    public void shouldParseMessageWithDuplicateFieldBeforeRepeatingGroup() throws Exception
    {
        setMemberIdOnGroup();
        setMemberIdField();

        encodeAndDecode();

        assertMemberIdField();
        assertMemberIdOnGroup();
    }

    @Test
    public void shouldNotParseMessageWithDuplicateFieldAfterRepeatingGroup() throws Exception
    {
        final String message = "8=FIXR.7.2\0019=54\00135=U2\00149=sender\00156=target\00134=1" +
            "\00152= \001101=1\001100=B\001100=A\00110=154\001";
        WRAPPER.decode(decoder, message);

        // Fails to parse A
        assertMemberIDAsString(decoder, null);
        assertMemberIdOnGroup();
    }

    private void assertMemberIdOnGroup() throws Exception
    {
        final Object memberIDsGroupDecoder = get(decoder, MEMBER_IDS_GROUP);
        assertMemberIDAsString(memberIDsGroupDecoder, "B");
    }

    private void assertMemberIdField() throws Exception
    {
        assertMemberIDAsString(decoder, "A");
    }

    private void encodeAndDecode()
    {
        setupHeader(encoder);
        WRAPPER.encode(encoder);
        WRAPPER.decode(decoder);
    }

    private void setMemberIdOnGroup() throws Exception
    {
        final Object membersEncoder = get(encoder, "members");
        final Object memberIDsGroupEncoder = get(membersEncoder, MEMBER_IDS_GROUP, 1);
        setMemberId(memberIDsGroupEncoder, "B");
    }

    private void setMemberIdField() throws Exception
    {
        setMemberId(encoder, "A");
    }

    private void assertMemberIDAsString(final Object memberIDsGroupDecoder, final String b) throws Exception
    {
        assertEquals(b, getString(memberIDsGroupDecoder, "memberIDAsString"));
    }

    private void setMemberId(final Object encoder, final String value) throws Exception
    {
        setCharSequence(encoder, "memberID", value);
    }
}
