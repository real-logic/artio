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

import iLinkBinary.*;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.DebugLogger;

import java.util.function.Consumer;

import static uk.co.real_logic.artio.LogTag.FIXP_BUSINESS;

public final class ILink3BusinessMessageDissector
{
    // Client -> Exchange
    private final NewOrderSingle514Decoder newOrderSingle = new NewOrderSingle514Decoder();
    private final OrderCancelReplaceRequest515Decoder orderCancelReplaceRequest =
        new OrderCancelReplaceRequest515Decoder();
    private final OrderCancelRequest516Decoder orderCancelRequest = new OrderCancelRequest516Decoder();
    private final MassQuote517Decoder massQuote = new MassQuote517Decoder();
    private final QuoteCancel528Decoder quoteCancel = new QuoteCancel528Decoder();
    private final OrderStatusRequest533Decoder orderStatusRequest = new OrderStatusRequest533Decoder();
    private final OrderMassStatusRequest530Decoder orderMassStatusRequest = new OrderMassStatusRequest530Decoder();
    private final OrderMassActionRequest529Decoder orderMassActionRequest = new OrderMassActionRequest529Decoder();
    private final NewOrderCross544Decoder newOrderCross = new NewOrderCross544Decoder();
    private final RequestForQuote543Decoder requestForQuote = new RequestForQuote543Decoder();
    private final SecurityDefinitionRequest560Decoder securityDefinitionRequest =
        new SecurityDefinitionRequest560Decoder();
    private final PartyDetailsDefinitionRequest518Decoder partyDetailsDefinitionRequest =
        new PartyDetailsDefinitionRequest518Decoder();
    private final ExecutionAck539Decoder executionAck = new ExecutionAck539Decoder();

    // Exchange -> Client
    private final PartyDetailsDefinitionRequestAck519Decoder partyDetailsDefinitionRequestAck =
        new PartyDetailsDefinitionRequestAck519Decoder();
    private final ExecutionReportNew522Decoder executionReportNew = new ExecutionReportNew522Decoder();
    private final ExecutionReportReject523Decoder executionReportReject = new ExecutionReportReject523Decoder();
    private final ExecutionReportElimination524Decoder executionReportElimination
        = new ExecutionReportElimination524Decoder();
    private final ExecutionReportTradeOutright525Decoder executionReportTradeOutright
        = new ExecutionReportTradeOutright525Decoder();
    private final ExecutionReportTradeSpread526Decoder executionReportTradeSpread
        = new ExecutionReportTradeSpread526Decoder();
    private final ExecutionReportTradeSpreadLeg527Decoder executionReportTradeSpreadLeg
        = new ExecutionReportTradeSpreadLeg527Decoder();
    private final ExecutionReportModify531Decoder executionReportModify = new ExecutionReportModify531Decoder();
    private final ExecutionReportStatus532Decoder executionReportStatus = new ExecutionReportStatus532Decoder();
    private final ExecutionReportCancel534Decoder executionReportCancel = new ExecutionReportCancel534Decoder();
    private final OrderCancelReject535Decoder orderCancelReject = new OrderCancelReject535Decoder();
    private final OrderCancelReplaceReject536Decoder orderCancelReplaceReject =
        new OrderCancelReplaceReject536Decoder();
    private final PartyDetailsListReport538Decoder partyDetailsListReport =
        new PartyDetailsListReport538Decoder();
    private final MassQuoteAck545Decoder massQuoteAck = new MassQuoteAck545Decoder();
    private final RequestForQuoteAck546Decoder requestForQuoteAck = new RequestForQuoteAck546Decoder();
    private final ExecutionReportTradeAddendumOutright548Decoder executionReportTradeAddendumOutright
        = new ExecutionReportTradeAddendumOutright548Decoder();
    private final ExecutionReportTradeAddendumSpread549Decoder executionReportTradeAddendumSpread
        = new ExecutionReportTradeAddendumSpread549Decoder();
    private final ExecutionReportTradeAddendumSpreadLeg550Decoder executionReportTradeAddendumSpreadLeg
        = new ExecutionReportTradeAddendumSpreadLeg550Decoder();
    private final SecurityDefinitionResponse561Decoder securityDefinitionResponse
        = new SecurityDefinitionResponse561Decoder();
    private final OrderMassActionReport562Decoder orderMassActionReport = new OrderMassActionReport562Decoder();
    private final QuoteCancelAck563Decoder quoteCancelAck = new QuoteCancelAck563Decoder();

    // Client -> Exchange
    private final Consumer<StringBuilder> newOrderSingleAppendTo = newOrderSingle::appendTo;
    private final Consumer<StringBuilder> orderCancelReplaceRequestAppendTo = orderCancelReplaceRequest::appendTo;
    private final Consumer<StringBuilder> orderCancelRequestAppendTo = orderCancelRequest::appendTo;
    private final Consumer<StringBuilder> massQuoteAppendTo = massQuote::appendTo;
    private final Consumer<StringBuilder> quoteCancelAppendTo = quoteCancel::appendTo;
    private final Consumer<StringBuilder> orderStatusRequestAppendTo = orderStatusRequest::appendTo;
    private final Consumer<StringBuilder> orderMassStatusRequestAppendTo = orderMassStatusRequest::appendTo;
    private final Consumer<StringBuilder> orderMassActionRequestAppendTo = orderMassActionRequest::appendTo;
    private final Consumer<StringBuilder> newOrderCrossAppendTo = newOrderCross::appendTo;
    private final Consumer<StringBuilder> requestForQuoteAppendTo = requestForQuote::appendTo;
    private final Consumer<StringBuilder> securityDefinitionRequestAppendTo = securityDefinitionRequest::appendTo;
    private final Consumer<StringBuilder> partyDetailsDefinitionRequestAppendTo =
        partyDetailsDefinitionRequest::appendTo;
    private final Consumer<StringBuilder> executionAckAppendTo = executionAck::appendTo;

    // Exchange -> Client
    private final Consumer<StringBuilder> partyDetailsDefinitionRequestAckAppendTo =
        partyDetailsDefinitionRequestAck::appendTo;
    private final Consumer<StringBuilder> executionReportNewAppendTo =
        executionReportNew::appendTo;
    private final Consumer<StringBuilder> executionReportRejectAppendTo =
        executionReportReject::appendTo;
    private final Consumer<StringBuilder> executionReportEliminationAppendTo =
        executionReportElimination::appendTo;
    private final Consumer<StringBuilder> executionReportTradeOutrightAppendTo =
        executionReportTradeOutright::appendTo;
    private final Consumer<StringBuilder> executionReportTradeSpreadAppendTo =
        executionReportTradeSpread::appendTo;
    private final Consumer<StringBuilder> executionReportTradeSpreadLegAppendTo =
        executionReportTradeSpreadLeg::appendTo;
    private final Consumer<StringBuilder> executionReportModifyAppendTo =
        executionReportModify::appendTo;
    private final Consumer<StringBuilder> executionReportStatusAppendTo =
        executionReportStatus::appendTo;
    private final Consumer<StringBuilder> executionReportCancelAppendTo =
        executionReportCancel::appendTo;
    private final Consumer<StringBuilder> orderCancelRejectAppendTo =
        orderCancelReject::appendTo;
    private final Consumer<StringBuilder> orderCancelReplaceRejectAppendTo =
        orderCancelReplaceReject::appendTo;
    private final Consumer<StringBuilder> partyDetailsListReportAppendTo =
        partyDetailsListReport::appendTo;
    private final Consumer<StringBuilder> massQuoteAckAppendTo =
        massQuoteAck::appendTo;
    private final Consumer<StringBuilder> requestForQuoteAckAppendTo =
        requestForQuoteAck::appendTo;
    private final Consumer<StringBuilder> executionReportTradeAddendumOutrightAppendTo =
        executionReportTradeAddendumOutright::appendTo;
    private final Consumer<StringBuilder> executionReportTradeAddendumSpreadAppendTo =
        executionReportTradeAddendumSpread::appendTo;
    private final Consumer<StringBuilder> executionReportTradeAddendumSpreadLegAppendTo =
        executionReportTradeAddendumSpreadLeg::appendTo;
    private final Consumer<StringBuilder> securityDefinitionResponseAppendTo =
        securityDefinitionResponse::appendTo;
    private final Consumer<StringBuilder> orderMassActionReportAppendTo =
        orderMassActionReport::appendTo;
    private final Consumer<StringBuilder> quoteCancelAckAppendTo =
        quoteCancelAck::appendTo;

    private final Logger logger;

    public ILink3BusinessMessageDissector()
    {
        this(ILink3BusinessMessageDissector::logDefault);
    }

    public ILink3BusinessMessageDissector(final Logger logger)
    {
        this.logger = logger;
    }

    public void onBusinessMessage(
        final int templateId,
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final boolean inbound)
    {
        onClientToExchangeMessage(templateId, buffer, offset, blockLength, version, inbound);

        onExchangeToClientMessage(templateId, buffer, offset, blockLength, version, inbound);
    }

    private void onExchangeToClientMessage(
        final int templateId,
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final boolean inbound)
    {
        switch (templateId)
        {
            case PartyDetailsDefinitionRequestAck519Decoder.TEMPLATE_ID:
                partyDetailsDefinitionRequestAck.wrap(buffer, offset, blockLength, version);
                log(inbound, partyDetailsDefinitionRequestAckAppendTo);
                break;
            case ExecutionReportNew522Decoder.TEMPLATE_ID:
                executionReportNew.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportNewAppendTo);
                break;
            case ExecutionReportReject523Decoder.TEMPLATE_ID:
                executionReportReject.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportRejectAppendTo);
                break;
            case ExecutionReportElimination524Decoder.TEMPLATE_ID:
                executionReportElimination.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportEliminationAppendTo);
                break;
            case ExecutionReportTradeOutright525Decoder.TEMPLATE_ID:
                executionReportTradeOutright.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportTradeOutrightAppendTo);
                break;
            case ExecutionReportTradeSpread526Decoder.TEMPLATE_ID:
                executionReportTradeSpread.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportTradeSpreadAppendTo);
                break;
            case ExecutionReportTradeSpreadLeg527Decoder.TEMPLATE_ID:
                executionReportTradeSpreadLeg.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportTradeSpreadLegAppendTo);
                break;
            case ExecutionReportModify531Decoder.TEMPLATE_ID:
                executionReportModify.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportModifyAppendTo);
                break;
            case ExecutionReportStatus532Decoder.TEMPLATE_ID:
                executionReportStatus.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportStatusAppendTo);
                break;
            case ExecutionReportCancel534Decoder.TEMPLATE_ID:
                executionReportCancel.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportCancelAppendTo);
                break;
            case OrderCancelReject535Decoder.TEMPLATE_ID:
                orderCancelReject.wrap(buffer, offset, blockLength, version);
                log(inbound, orderCancelRejectAppendTo);
                break;
            case OrderCancelReplaceReject536Decoder.TEMPLATE_ID:
                orderCancelReplaceReject.wrap(buffer, offset, blockLength, version);
                log(inbound, orderCancelReplaceRejectAppendTo);
                break;
            case PartyDetailsListReport538Decoder.TEMPLATE_ID:
                partyDetailsListReport.wrap(buffer, offset, blockLength, version);
                log(inbound, partyDetailsListReportAppendTo);
                break;
            case MassQuoteAck545Decoder.TEMPLATE_ID:
                massQuoteAck.wrap(buffer, offset, blockLength, version);
                log(inbound, massQuoteAckAppendTo);
                break;
            case RequestForQuoteAck546Decoder.TEMPLATE_ID:
                requestForQuoteAck.wrap(buffer, offset, blockLength, version);
                log(inbound, requestForQuoteAckAppendTo);
                break;
            case ExecutionReportTradeAddendumOutright548Decoder.TEMPLATE_ID:
                executionReportTradeAddendumOutright.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportTradeAddendumOutrightAppendTo);
                break;
            case ExecutionReportTradeAddendumSpread549Decoder.TEMPLATE_ID:
                executionReportTradeAddendumSpread.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportTradeAddendumSpreadAppendTo);
                break;
            case ExecutionReportTradeAddendumSpreadLeg550Decoder.TEMPLATE_ID:
                executionReportTradeAddendumSpreadLeg.wrap(buffer, offset, blockLength, version);
                log(inbound, executionReportTradeAddendumSpreadLegAppendTo);
                break;
            case SecurityDefinitionResponse561Decoder.TEMPLATE_ID:
                securityDefinitionResponse.wrap(buffer, offset, blockLength, version);
                log(inbound, securityDefinitionResponseAppendTo);
                break;
            case OrderMassActionReport562Decoder.TEMPLATE_ID:
                orderMassActionReport.wrap(buffer, offset, blockLength, version);
                log(inbound, orderMassActionReportAppendTo);
                break;
            case QuoteCancelAck563Decoder.TEMPLATE_ID:
                quoteCancelAck.wrap(buffer, offset, blockLength, version);
                log(inbound, quoteCancelAckAppendTo);
                break;
        }
    }

    private void onClientToExchangeMessage(
        final int templateId,
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final boolean inbound)
    {
        switch (templateId)
        {
            case NewOrderSingle514Decoder.TEMPLATE_ID:
                newOrderSingle.wrap(buffer, offset, blockLength, version);
                log(inbound, newOrderSingleAppendTo);
                break;
            case OrderCancelReplaceRequest515Decoder.TEMPLATE_ID:
                orderCancelReplaceRequest.wrap(buffer, offset, blockLength, version);
                log(inbound, orderCancelReplaceRequestAppendTo);
                break;
            case OrderCancelRequest516Decoder.TEMPLATE_ID:
                orderCancelRequest.wrap(buffer, offset, blockLength, version);
                log(inbound, orderCancelRequestAppendTo);
                break;
            case MassQuote517Decoder.TEMPLATE_ID:
                massQuote.wrap(buffer, offset, blockLength, version);
                log(inbound, massQuoteAppendTo);
                break;
            case QuoteCancel528Decoder.TEMPLATE_ID:
                quoteCancel.wrap(buffer, offset, blockLength, version);
                log(inbound, quoteCancelAppendTo);
                break;
            case OrderStatusRequest533Decoder.TEMPLATE_ID:
                orderStatusRequest.wrap(buffer, offset, blockLength, version);
                log(inbound, orderStatusRequestAppendTo);
                break;
            case OrderMassStatusRequest530Decoder.TEMPLATE_ID:
                orderMassStatusRequest.wrap(buffer, offset, blockLength, version);
                log(inbound, orderMassStatusRequestAppendTo);
                break;
            case OrderMassActionRequest529Decoder.TEMPLATE_ID:
                orderMassActionRequest.wrap(buffer, offset, blockLength, version);
                log(inbound, orderMassActionRequestAppendTo);
                break;
            case NewOrderCross544Decoder.TEMPLATE_ID:
                newOrderCross.wrap(buffer, offset, blockLength, version);
                log(inbound, newOrderCrossAppendTo);
                break;
            case RequestForQuote543Decoder.TEMPLATE_ID:
                requestForQuote.wrap(buffer, offset, blockLength, version);
                log(inbound, requestForQuoteAppendTo);
                break;
            case SecurityDefinitionRequest560Decoder.TEMPLATE_ID:
                securityDefinitionRequest.wrap(buffer, offset, blockLength, version);
                log(inbound, securityDefinitionRequestAppendTo);
                break;
            case PartyDetailsDefinitionRequest518Decoder.TEMPLATE_ID:
                partyDetailsDefinitionRequest.wrap(buffer, offset, blockLength, version);
                log(inbound, partyDetailsDefinitionRequestAppendTo);
                break;
            case ExecutionAck539Decoder.TEMPLATE_ID:
                executionAck.wrap(buffer, offset, blockLength, version);
                log(inbound, executionAckAppendTo);
                break;
        }
    }

    private void log(final boolean inbound, final Consumer<StringBuilder> appendTo)
    {
        logger.log(inbound ? "> " : "< ", appendTo);
    }

    private static void logDefault(final String prefix, final Consumer<StringBuilder> appendTo)
    {
        DebugLogger.logSbeDecoder(FIXP_BUSINESS, prefix, appendTo);
    }

    public interface Logger
    {
        void log(String prefix, Consumer<StringBuilder> appendTo);
    }
}
