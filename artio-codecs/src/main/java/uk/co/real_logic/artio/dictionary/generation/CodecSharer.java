/*
 * Copyright 2021 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import uk.co.real_logic.artio.dictionary.DictionaryParser;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CodecSharer
{
    private final List<Dictionary> inputDictionaries;

    private final Map<String, Field> sharedNameToField = new HashMap<>();

    CodecSharer(final List<Dictionary> inputDictionaries)
    {
        this.inputDictionaries = inputDictionaries;
    }

    public void share()
    {
        final Dictionary firstDictionary = inputDictionaries.get(0);

        findSharedFields();
        // TODO: remap fields when copying

        final Set<String> commonMessageNames = findCommonMessageNames();
        final Map<String, Message> nameToMessage = new HashMap<>();
        for (final Dictionary dictionary : inputDictionaries)
        {
            dictionary.messages().forEach(msg ->
            {
                final String name = msg.name();
                if (commonMessageNames.contains(name))
                {
                    Message sharedMessage = nameToMessage.get(name);
                    if (sharedMessage == null)
                    {
                        nameToMessage.put(name, copyOf(msg));
                    }
                    else
                    {
                        // merge fields
                        if (msg.packedType() != sharedMessage.packedType())
                        {
                            // TODO: do we care? Maybe we should put a marker type or something?
                            System.err.println("Invalid types: ");
                            System.err.println(msg);
                            System.err.println(sharedMessage);
                        }
                        else
                        {
                            // Still need to merge aggregates even though we have merged fields
                            // As some fields may be common to different dictionaries but not messages
                            mergeAggregate(msg, sharedMessage);
                        }
                    }
                }
            });
        }

        final Map<String, Component> components = new HashMap<>();
        final Component header = sharedComponent(Dictionary::header);
        final Component trailer = sharedComponent(Dictionary::trailer);
        final String specType = DictionaryParser.DEFAULT_SPEC_TYPE;
        final int majorVersion = 0;
        final int minorVersion = 0;

        final List<Message> messages = new ArrayList<>(nameToMessage.values());
        components.putAll(firstDictionary.components());

        final Dictionary sharedDictionary = new Dictionary(
            messages,
            sharedNameToField,
            components,
            header,
            trailer,
            specType,
            majorVersion,
            minorVersion);

        sharedDictionary.shared(true);
        inputDictionaries.forEach(dict -> connectToSharedDictionary(sharedDictionary, dict));
        inputDictionaries.add(sharedDictionary);
    }

    private void connectToSharedDictionary(final Dictionary sharedDictionary, final Dictionary dict)
    {
        dict.sharedParent(sharedDictionary);
    }

    private void findSharedFields()
    {
        final Set<String> commonNonEnumFieldNames = findCommonNonEnumFieldNames();
        final Set<String> allEnumFieldNames = allEnumFieldNames();

        for (final Dictionary dictionary : inputDictionaries)
        {
            final Map<String, Field> fields = dictionary.fields();
            commonNonEnumFieldNames.forEach(fieldName -> mergeField(fields, fieldName));
            allEnumFieldNames.forEach(enumName -> mergeField(fields, enumName));
        }
    }

    private void mergeField(final Map<String, Field> fields, final String fieldName)
    {
        sharedNameToField.compute(fieldName, (name, sharedField) ->
        {
            final Field field = fields.get(name);
            if (field == null)
            {
                return sharedField;
            }

            if (sharedField == null)
            {
                return copyOf(field);
            } else
            {
                // TODO: merge fields and check collissions
                return sharedField;
            }
        });
    }

    private Set<String> allEnumFieldNames()
    {
        return inputDictionaries
            .stream()
            .flatMap(dict -> fieldNames(dict, true))
            .collect(Collectors.toSet());
    }

    private Field copyOf(final Field field)
    {
        final Field newField = new Field(field.number(), field.name(), field.type());
        newField.values().addAll(field.values());
        return newField;
    }

    private Component sharedComponent(final Function<Dictionary, Component> getter)
    {
        final Dictionary firstDictionary = inputDictionaries.get(0);
        final Component sharedComponent = copyOf(getter.apply(firstDictionary));
        inputDictionaries.forEach(dict ->
        {
            final Component component = getter.apply(dict);
            mergeAggregate(component, sharedComponent);
        });
        return sharedComponent;
    }

    private void mergeAggregate(final Aggregate aggregate, final Aggregate sharedAggregate)
    {
        final Map<String, Entry> nameToEntry = nameToEntry(aggregate.entries());
        final Iterator<Entry> it = sharedAggregate.entries().iterator();
        while (it.hasNext())
        {
            final Entry sharedEntry = it.next();
            final Entry entry = nameToEntry.get(sharedEntry.name());
            if (entry == null)
            {
                it.remove();
            }
            else
            {
                // Only required if all are required
                sharedEntry.required(sharedEntry.required() && entry.required());

                // TODO: check collisions
            }
        }
    }

    private Set<String> findCommonMessageNames()
    {
        return findCommonNames(this::messageNames);
    }

    private Set<String> findCommonNonEnumFieldNames()
    {
        return findCommonNames(dictionary -> fieldNames(dictionary, false).collect(Collectors.toSet()));
    }

    private Set<String> findCommonNames(final Function<Dictionary, Set<String>> namesGetter)
    {
        final Set<String> messageNames = new HashSet<>();
        inputDictionaries.forEach(dict ->
        {
            final Set<String> namesInDictionary = namesGetter.apply(dict);
            if (messageNames.isEmpty())
            {
                messageNames.addAll(namesInDictionary);
            }
            else
            {
                messageNames.retainAll(namesInDictionary);
            }
        });

        return messageNames;
    }

    private Set<String> messageNames(final Dictionary dictionary)
    {
        return dictionary.messages().stream().map(Message::name).collect(Collectors.toSet());
    }

    private Stream<String> fieldNames(final Dictionary dictionary, final boolean isEnum)
    {
        return dictionary.fields().entrySet()
            .stream()
            .filter(e -> e.getValue().isEnum() == isEnum)
            .map(Map.Entry::getKey);
    }

    private Map<String, Entry> nameToEntry(final List<Entry> entries)
    {
        return entries.stream().collect(Collectors.toMap(Entry::name, x -> x));
    }

    private Entry copyOf(final Entry entry)
    {
        return new Entry(entry.required(), entry.element());
    }

    private Component copyOf(final Component component)
    {
        final Component newComponent = new Component(component.name());
        copyOf(component, newComponent);
        return newComponent;
    }

    private Message copyOf(final Message message)
    {
        final Message newMessage = new Message(message.name(), message.fullType(), message.category());
        copyOf(message, newMessage);
        return newMessage;
    }

    private void copyOf(final Aggregate aggregate, final Aggregate newAggregate)
    {
        for (final Entry entry : aggregate.entries())
        {
            newAggregate.entries().add(copyOf(entry));
        }
    }

}
