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
package uk.co.real_logic.fix_gateway.builder;

import sun.nio.ch.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.flyweight_api.*;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.util.Currency;

/**
 * .
 */
public class OrderSingleEncoder implements Encoder
{
    private DirectBuffer buffer;

    private AsciiFlyweight clOrdID;
    private char handlInst;
    private Side side;
    private OrdType ordType;
    private long transactTime;
    private AsciiFlyweight symbol;

    private boolean hasClientID;
    private AsciiFlyweight clientID;

    private boolean hasExecBroker;
    private AsciiFlyweight execBroker;

    private boolean hasAccount;
    private AsciiFlyweight account;

    private boolean hasNoAllocs;
    private int noAllocs;

    private boolean hasAllocAccount;
    private AsciiFlyweight allocAccount;

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
    private AsciiFlyweight tradingSessionID;

    private boolean hasProcessCode;
    private char processCode;

    private boolean hasSymbolSfx;
    private AsciiFlyweight symbolSfx;

    private boolean hasSecurityID;
    private AsciiFlyweight securityID;

    private boolean hasIDSource;
    private AsciiFlyweight iDSource;

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
    private AsciiFlyweight issuer;

    private boolean hasEncodedIssuerLen;
    private int encodedIssuerLen;

    private boolean hasSecurityDesc;
    private AsciiFlyweight securityDesc;

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
    private AsciiFlyweight complianceID;

    private boolean hasSolicitedFlag;
    private boolean solicitedFlag;

    private boolean hasIOIid;
    private AsciiFlyweight iOIid;

    private boolean hasQuoteID;
    private AsciiFlyweight quoteID;

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

    public OrderSingleEncoder()
    {
    }

    public int encode(final MutableDirectBuffer buffer, final int offset)
    {
        return 0;
    }

    public void clOrdID(final String clOrdID)
    {
        // TODO
    }

    public void clOrdID(final AsciiFlyweight clOrdID)
    {
        // TODO
    }

    public void clOrdID(final DirectBuffer clOrdID, final int offset, final int length)
    {
        // TODO
    }

    public void handlInst(final char handlInst)
    {
        this.handlInst = handlInst;
    }

    public void side(final Side side)
    {
        this.side = side;
    }

    public void ordType(final OrdType ordType)
    {
        this.ordType = ordType;
    }

    public void transactTime(final long transactTime)
    {
        this.transactTime = transactTime;
    }

    public void symbol(final String symbol)
    {
    }
}
