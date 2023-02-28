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
package uk.co.real_logic.artio.builder;

public final class RejectUnknownEnumValue
{
    private static final String CODEC_DISABLE_REJECT_UNKNOWN_ENUM_VALUE_PROP =
        "fix.codecs.disable_reject_unknown_enum_value";
    private static final boolean CODEC_DISABLE_REJECT_UNKNOWN_ENUM_VALUE_ENABLED =
        Boolean.getBoolean(CODEC_DISABLE_REJECT_UNKNOWN_ENUM_VALUE_PROP);
    public static final boolean CODEC_REJECT_UNKNOWN_ENUM_VALUE_ENABLED =
        !CODEC_DISABLE_REJECT_UNKNOWN_ENUM_VALUE_ENABLED;
}
