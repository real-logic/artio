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
package uk.co.real_logic.fix_gateway.dictionary;

import org.w3c.dom.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Value;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static javax.xml.xpath.XPathConstants.NODESET;

public class DictionaryParser
{
    private static final String FIELD_EXPR = "/fix/fields/field";
    private static final String MESSAGE_EXPR = "/fix/messages/message";
    private static final String HEADER_EXPR = "/fix/header/field";
    private static final String TRAILER_EXPR = "/fix/trailer/field";

    private final DocumentBuilder documentBuilder;
    private final XPathExpression findField;
    private final XPathExpression findMessage;
    private final XPathExpression findHeader;
    private final XPathExpression findTrailer;

    public DictionaryParser()
    {
        try
        {
            documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            final XPath xPath = XPathFactory.newInstance().newXPath();
            findField = xPath.compile(FIELD_EXPR);
            findMessage = xPath.compile(MESSAGE_EXPR);
            findHeader = xPath.compile(HEADER_EXPR);
            findTrailer = xPath.compile(TRAILER_EXPR);
        }
        catch (ParserConfigurationException | XPathExpressionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public DataDictionary parse(final InputStream in) throws Exception
    {
        final Document document = documentBuilder.parse(in);
        final Map<String, Field> fields = parseFields(document);
        final List<Message> messages = parseMessages(document, fields);
        extractCommonFields(document, messages, fields);
        return new DataDictionary(messages, fields);
    }

    private Map<String, Field> parseFields(final Document document) throws XPathExpressionException
    {
        final HashMap<String, Field> fields = new HashMap<>();
        extractNodes(document, findField, node ->
        {
            final NamedNodeMap attributes = node.getAttributes();

            final int number = Integer.parseInt(getValue(attributes, "number"));
            final String name = getValue(attributes, "name");
            final Type type = Type.valueOf(getValue(attributes, "type"));
            final Field field = new Field(number, name, type);

            extractEnumValues(field.values(), node.getChildNodes());
            fields.put(name, field);
        });
        return fields;
    }

    private void extractEnumValues(final List<Value> values, final NodeList childNodes)
    {
        forEach(childNodes, node ->
        {
            final NamedNodeMap attributes = node.getAttributes();
            final char representation = getValue(attributes, "enum").charAt(0);
            final String description = getValue(attributes, "description");
            values.add(new Value(representation, description));
        });
    }

    private List<Message> parseMessages(final Document document, final Map<String, Field> fields) throws XPathExpressionException
    {
        final ArrayList<Message> messages = new ArrayList<>();

        extractNodes(document, findMessage, node ->
        {
            final NamedNodeMap attributes = node.getAttributes();

            final String name = getValue(attributes, "name");
            final char type = getValue(attributes, "msgtype").charAt(0);
            final Category category = parseCategory(getValue(attributes, "msgcat"));
            final Message message = new Message(name, type, category);

            extractChildFields(node.getChildNodes(), fields, message);
            extractGroups(node.getChildNodes(), fields, message.groups());

            messages.add(message);
        });

        return messages;
    }

    private void extractGroups(final NodeList childNodes, final Map<String, Field> fields, final List<Group> groups)
    {
        forEach(childNodes, node ->
        {
            if ("group".equals(node.getNodeName()))
            {
                final NamedNodeMap attributes = node.getAttributes();

                final String name = getValue(attributes, "name");
                final Group group = new Group(name, isRequired(attributes));

                extractChildFields(node.getChildNodes(), fields, group);

                groups.add(group);
            }
        });
    }

    private void extractChildFields(final NodeList childNodes, final Map<String, Field> fields, final Entry entry)
    {
        forEach(childNodes, extractField(entry.requiredFields(), entry.optionalFields(), fields));
    }

    private void extractCommonFields(final Document document, final List<Message> messages, final Map<String, Field> fields)
            throws XPathExpressionException
    {
        addFields(document, messages, fields, (left, right) -> left.addAll(0, right), findHeader);
        addFields(document, messages, fields, List::addAll, findTrailer);
    }

    private void addFields(
            final Document document,
            final List<Message> messages,
            final Map<String, Field> fields,
            final BiConsumer<List<Field>, List<Field>> merge,
            final XPathExpression expression)
            throws XPathExpressionException
    {
        final List<Field> requiredFields = new ArrayList<>();
        final List<Field> optionalFields = new ArrayList<>();
        extractNodes(document, expression, extractField(requiredFields, optionalFields, fields));

        messages.forEach(message ->
        {
            merge.accept(message.optionalFields(), optionalFields);
            merge.accept(message.requiredFields(), requiredFields);
        });
    }

    private Consumer<Node> extractField(
            final List<Field> requiredFields, final List<Field> optionalFields, final Map<String, Field> fields)
    {
        return node ->
        {
            if ("field".equals(node.getNodeName()))
            {
                final NamedNodeMap attributes = node.getAttributes();

                final String name = getValue(attributes, "name");
                final Field field = fields.get(name);

                (isRequired(attributes) ? requiredFields : optionalFields).add(field);
            }
        };
    }

    private boolean isRequired(final NamedNodeMap attributes)
    {
        return "Y".equals(getValue(attributes, "required"));
    }

    private Category parseCategory(final String from)
    {
        return Category.valueOf(from.toUpperCase());
    }

    private String getValue(final NamedNodeMap attributes, final String attributeName)
    {
        return attributes.getNamedItem(attributeName).getNodeValue();
    }

    private void extractNodes(
            final Document document, final XPathExpression expression, final Consumer<Node> handler)
            throws XPathExpressionException
    {
        forEach((NodeList) expression.evaluate(document, NODESET), handler);
    }

    private void forEach(final NodeList nodes, final Consumer<Node> handler)
    {
        for (int i = 0; i < nodes.getLength(); i++)
        {
            final Node node = nodes.item(i);
            if (node instanceof Element)
            {
                handler.accept(node);
            }
        }
    }

}
