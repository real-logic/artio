/*
 * Copyright 2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.fixp;

/**
 * Class that stores additional information related to FixP messages.
 */
public class FixPMessageHeader
{
    private int messageSize;

    /**
     * Gets the full size of the message, from the SOFH, including variable length fields.
     *
     * @return the full size of the message
     */
    public int messageSize()
    {
        return messageSize;
    }

    public void messageSize(final int messageSize)
    {
        this.messageSize = messageSize;
    }
}
