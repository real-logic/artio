/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.library.validation;

import uk.co.real_logic.fix_gateway.decoder.Constants;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;

import static uk.co.real_logic.fix_gateway.fields.RejectReason.COMPID_PROBLEM;

/**
 * A message validation strategy that checks the target comp id of each message.
 */
public final class TargetCompIdValidationStrategy implements MessageValidationStrategy
{
    private final char[] gatewayCompId;

    public TargetCompIdValidationStrategy(final String gatewayCompId)
    {
        this(gatewayCompId.toCharArray());
    }

    public TargetCompIdValidationStrategy(final char[] gatewayCompId)
    {
        this.gatewayCompId = gatewayCompId;
    }

    public boolean validate(final HeaderDecoder header)
    {
        return CodecUtil.equals(gatewayCompId, header.targetCompID(), header.targetCompIDLength());
    }

    public int invalidTagId()
    {
        return Constants.TARGET_COMP_ID;
    }

    public int rejectReason()
    {
        return COMPID_PROBLEM.representation();
    }
}
