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
package uk.co.real_logic.fix_gateway.generated_api;

import sun.nio.ch.DirectBuffer;
import uk.co.real_logic.fix_gateway.util.StringFlyweight;

import java.util.Currency;

/**
 * .
 */
public class OrderSingleDecoder
{

    private final DirectBuffer buffer;

    private StringFlyweight clOrdID;
    private char handlInst;
    private Side side;
    private OrdType ordType;
    private long transactTime;
    private StringFlyweight symbol;

    public StringFlyweight clOrdID()
    {
        return clOrdID;
    }

    public char handlInst()
    {
        return handlInst;
    }

    public Side side()
    {
        return side;
    }

    public OrdType ordType()
    {
        return ordType;
    }

    public long transactTime()
    {
        return transactTime;
    }

    public StringFlyweight symbol()
    {
        return symbol;
    }

    private boolean hasClientID;
    private StringFlyweight clientID;

    private boolean hasExecBroker;
    private StringFlyweight execBroker;

    private boolean hasAccount;
    private StringFlyweight account;

    private boolean hasNoAllocs;
    private int noAllocs;

    private boolean hasAllocAccount;
    private StringFlyweight allocAccount;

    private boolean hasAllocShares;
    private int allocShares;

    private boolean hasSettlmntTyp;
    private char settlmntTyp;

    private boolean hasFutSettDate;
    private long futSettDate;

    private boolean hasExecInst;
    private ExecInst execInst;

    private boolean hasMinQty;
    private Qty minQty;

    private boolean hasMaxFloor;
    private Qty maxFloor;

    private boolean hasExDestination;
    private Exchange exDestination;

    private boolean hasNoTradingSessions;
    private int noTradingSessions;

    private boolean hasTradingSessionID;
    private StringFlyweight tradingSessionID;

    private boolean hasProcessCode;
    private char processCode;

    private boolean hasSymbolSfx;
    private StringFlyweight symbolSfx;

    private boolean hasSecurityID;
    private StringFlyweight securityID;

    private boolean hasIDSource;
    private StringFlyweight iDSource;

    private boolean hasMaturityMonthYear;
    private MonthYear maturityMonthYear;

    private boolean hasMaturityDay;
    private int maturityDay;

    private boolean hasPutOrCall;
    private int putOrCall;

    private boolean hasStrikePrice;
    private int strikePrice;

    private boolean hasOptAttribute;
    private char optAttribute;

    private boolean hasContractMultiplier;
    private float contractMultiplier;

    private boolean hasCouponRate;
    private float couponRate;

    private boolean hasSecurityExchange;
    private Exchange securityExchange;

    private boolean hasIssuer;
    private StringFlyweight issuer;

    private boolean hasEncodedIssuerLen;
    private int encodedIssuerLen;

    private boolean hasSecurityDesc;
    private StringFlyweight securityDesc;

    private boolean hasEncodedSecurityDescLen;
    private int encodedSecurityDescLen;

    private boolean hasPrevClosePx;
    private int prevClosePx;

    private boolean hasLocateReqd;
    private boolean locateReqd;

    private boolean hasOrderQty;
    private Qty orderQty;

    private boolean hasCashOrderQty;
    private Qty cashOrderQty;

    private boolean hasPrice;
    private long price;

    private boolean hasStopPx;
    private long stopPx;

    private boolean hasCurrency;
    private Currency currency;

    private boolean hasComplianceID;
    private StringFlyweight complianceID;

    private boolean hasSolicitedFlag;
    private boolean solicitedFlag;

    private boolean hasIOIid;
    private StringFlyweight iOIid;

    private boolean hasQuoteID;
    private StringFlyweight quoteID;

    private boolean hasEffectiveTime;
    private long effectiveTime;

    private boolean hasExpireDate;
    private long expireDate;

    private boolean hasExpireTime;
    private long expireTime;

    private boolean hasGTBookingInst;
    private int gTBookingInst;

    private boolean hasCommission;
    private Amt commission;

    private boolean hasCommType;
    private CommType commType;

    public OrderSingleDecoder(final DirectBuffer buffer)
    {
        this.buffer = buffer;
    }

    public void decode(final int offset, final int length)
    {

    }

}
