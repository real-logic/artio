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
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Value;
import uk.co.real_logic.fix_gateway.dictionary.ir.Message;

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
import java.util.function.Consumer;

import static javax.xml.xpath.XPathConstants.NODESET;

public class DictionaryParser
{
    private static final String FIELD_EXPR = "/fix/fields/field";

    private final DocumentBuilder documentBuilder;
    private final XPath xPath;
    private final XPathExpression findField;

    public DictionaryParser()
    {
        try
        {
            documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            xPath = XPathFactory.newInstance().newXPath();
            findField = xPath.compile(FIELD_EXPR);
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
        return new DataDictionary(messages, fields);
    }

    private Map<String, Field> parseFields(final Document document) throws XPathExpressionException
    {
        final HashMap<String, Field> fields = new HashMap<>();
        extractNodes(document, findField, node -> {
            final NamedNodeMap attributes = node.getAttributes();

            final int number = Integer.parseInt(getValue(attributes, "number"));
            final String name = getValue(attributes, "name");
            final Type type = Type.valueOf(getValue(attributes, "type"));
            final Field field = new Field(number, name, type);

            extractChildren(field.values(), node.getChildNodes());
            fields.put(name, field);
        });
        return fields;
    }

    private void extractChildren(final List<Value> values, final NodeList childNodes)
    {
        forEach(childNodes, node -> {
            if (node instanceof Element)
            {
                final NamedNodeMap attributes = node.getAttributes();
                final char representation = getValue(attributes, "enum").charAt(0);
                final String description = getValue(attributes, "description");
                values.add(new Value(representation, description));
            }
        });
    }

    private List<Message> parseMessages(final Document document, final Map<String, Field> fields)
    {
        return new ArrayList<>();
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
            handler.accept(nodes.item(i));
        }
    }

}
