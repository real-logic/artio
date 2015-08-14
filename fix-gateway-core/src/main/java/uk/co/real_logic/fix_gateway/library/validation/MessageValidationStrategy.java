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
package uk.co.real_logic.fix_gateway.library.validation;

import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;

public interface MessageValidationStrategy
{
    boolean validate(final HeaderDecoder header);

    int invalidTagId();

    int rejectReason();

    default MessageValidationStrategy and(MessageValidationStrategy other)
    {
        final MessageValidationStrategy self = this;
        return new MessageValidationStrategy()
        {
            private int invalidTagId;
            private int rejectReason;

            public boolean validate(final HeaderDecoder header)
            {
                final boolean selfValid = self.validate(header);

                if (selfValid)
                {
                    final boolean otherValid = other.validate(header);
                    if (otherValid)
                    {
                        return true;
                    }
                    else
                    {
                        invalidTagId = other.invalidTagId();
                        rejectReason = other.rejectReason();
                    }
                }
                else
                {
                    invalidTagId = self.invalidTagId();
                    rejectReason = self.rejectReason();
                }

                return false;
            }

            public int invalidTagId()
            {
                return invalidTagId;
            }

            public int rejectReason()
            {
                return rejectReason;
            }
        };
    }
}
