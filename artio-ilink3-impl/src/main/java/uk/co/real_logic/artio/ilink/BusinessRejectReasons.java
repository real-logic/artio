/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.ilink;

public final class BusinessRejectReasons
{
    /**
     * Looks up a human readable string version of the business reject messages' reason field.
     *
     * @param businessRejectReason the businessRejectReason field of the Business Reject message.
     * @return a human readable String corresponding to the error code.
     */
    public static String readableReason(final int businessRejectReason)
    {
        switch (businessRejectReason)
        {
            case 0: return "Other";
            case 1: return "Unknown ID (unknown PartyDetailsListReqID being referenced in business message)";
            case 2: return "Unknown Security";
            case 3: return "Unsupported Message Type (for example using messages specific to cash markets for futures)";
            case 5: return "Required Field Missing";
            case 6: return "Not Authorized";
            case 8: return "Throttle Limit Exceeded (volume controls)";
            case 100: return "Value is incorrect (out of range) for this tag (for example using a value outside " +
                "defined range of integer)";
            case 101: return "Incorrect data format for value (for example using ASCII character outside 32-126 in " +
                "string field)";
            case 102: return "Rejected due to Kill Switch";
            case 103: return "Rejected due to Risk Management API";
            case 104: return "Rejected due to Globex Credit Controls";
            case 105: return "Not Authorized to trade Deliverable Swap Futures";
            case 106: return "Not Authorized to trade Interest Rate Swap Futures";
            case 107: return "Rejected due to Inline Credit Controls";
            case 108: return "Invalid PartyDetailsListReqID (reusing already existing PartyDetailsListReqID while " +
                "creating Party Details Definition Request)";
            case 109: return "Incoming message could not be decoded";
            case 110: return "Same repeating group entry appears more than once";
            case 111: return "Exceeded maximum number of allowable Party Details Definition Requests";
            case 112: return "Technical Error in Registering Party Details";
            case 113: return "Rejected due to Cross Venue Risk";
            case 114: return "Order Status Not Available";
            case 115: return "Enum Not Supported";
            case 116: return "Order Status Not Found";
            case 117: return "Mass Order Status Cannot be Completed";
            case 118: return "Exceeded Maximum Number of Allowable RequestingPartyID's in Party Details List Request";
            case 119: return "Duplicate Order Threshold Exceeded";
            case 120: return "On-Demand Message Rejected Due to Corresponding PartyDetailsDefinitionRequest Being " +
                "Rejected";
            case 121: return "Message Rejected Since PartyDetailsListReqID Does Not Match with Corresponding " +
                "PartyDetailsDefinitionRequest as part of On-demand Message";
            case 122: return "Party Details Definition Request sent to MSGW Being Rejected Due to Corresponding " +
                "Business Message Being Rejected";
            case 123: return "Another Message Sent in Between Party Details Definition Request and Business Message " +
                "when using On-demand administrative information";
            case 124: return "Cannot Have More Than One In-Flight Mass Order Status Request in Progress";
            case 125: return "Exceeded Maximum Number of In-Flight Order Status Requests";
            case 126: return "Cannot Have More Than One In-Flight Party Details List Request in Progress";
            case 127: return "Party Details List Request is Missing Requesting Party ID and Party ID";
            case 128: return "Party Details List Request cannot contain both RequestingPartyID and PartyID";
            case 129: return "Party Details Definition Request Being Rejected Since Another Message was sent in " +
                "Between On-Demand Message";
            case 130: return "Buy Side Firm ID Does Not Match Sell Side Firm ID in New Order Cross";
            case 131: return "Message Type Not Supported on Backup Instance";
            case 132: return "New Order Cross Does Not Contain Buy Side Followed by Sell Side";
            default: return "Unknown";
        }
    }
}
