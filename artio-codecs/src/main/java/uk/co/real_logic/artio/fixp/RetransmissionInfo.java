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

public class RetransmissionInfo
{
    private long timestampInNs;
    private long fromSeqNo;
    private long count;
    private Object rejectionCode;

    /**
     * Call this method to reject the retransmit request.
     *
     * @param rejectionCode the code to return to the user.
     * @throws IllegalArgumentException if the <code>rejectionCode</code> isn't the correct reject code.
     */
    public void reject(final Object rejectionCode)
    {
        this.rejectionCode = rejectionCode;
    }

    /**
     * Gets the rejection code of the retransmission request is invalid or null otherwise. The type of the
     * rejection code will be specific to the protocol involved.
     *
     * @return the rejection code of the retransmission request.
     */
    public Object rejectionCode()
    {
        return rejectionCode;
    }

    public long timestampInNs()
    {
        return timestampInNs;
    }

    public long fromSeqNo()
    {
        return fromSeqNo;
    }

    public long count()
    {
        return count;
    }

    protected void initRequest(final long timestampInNs, final long fromSeqNo, final long count)
    {
        this.rejectionCode = null;
        this.timestampInNs = timestampInNs;
        this.fromSeqNo = fromSeqNo;
        this.count = count;
    }
}
