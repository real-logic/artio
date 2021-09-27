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
import uk.co.real_logic.artio.dictionary.ir.Field.Value;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

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
        final List<Message> messages = findSharedMessages();
        final Map<String, Component> components = new HashMap<>();
        final Component header = findSharedComponent(Dictionary::header);
        final Component trailer = findSharedComponent(Dictionary::trailer);
        final String specType = DictionaryParser.DEFAULT_SPEC_TYPE;
        final int majorVersion = 0;
        final int minorVersion = 0;


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

    private List<Message> findSharedMessages()
    {
        final Map<String, Message> nameToMessage = new HashMap<>();
        final Set<String> commonMessageNames = findCommonMessageNames();
        for (final Dictionary dictionary : inputDictionaries)
        {
            dictionary.messages().forEach(msg ->
            {
                final String name = msg.name();
                if (commonMessageNames.contains(name))
                {
                    final Message sharedMessage = nameToMessage.get(name);
                    if (sharedMessage == null)
                    {
                        msg.isInParent(true);
                        nameToMessage.put(name, copyOf(msg));
                    }
                    else
                    {
                        // merge fields
                        if (msg.packedType() != sharedMessage.packedType())
                        {
                            // TODO: if it happens then push the message type down into the implementations
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

        identifyMessageEntriesInParent(nameToMessage);

        return new ArrayList<>(nameToMessage.values());
    }

    private void identifyMessageEntriesInParent(final Map<String, Message> nameToMessage)
    {
        inputDictionaries.forEach(dictionary ->
        {
            dictionary.messages().forEach(msg ->
            {
                final String name = msg.name();
                final Message sharedMessage = nameToMessage.get(name);
                if (sharedMessage != null)
                {
                    identifyAggregateEntriesInParent(msg, sharedMessage);
                }
            });
        });
    }

    private void identifyAggregateEntriesInParent(final Aggregate agg, final Aggregate sharedAgg)
    {
        sharedAgg.entries().forEach(sharedEntry ->
        {
            agg.entries().forEach(entry ->
            {
                if (entry.name().equals(sharedEntry.name()))
                {
                    entry.isInParent(true);
                }
            });
        });
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

        formUnionEnums();

        sharedNameToField.values().forEach(field ->
            DictionaryParser.checkAssociatedLengthField(sharedNameToField, field, "CodecSharer"));
    }

    private void formUnionEnums()
    {
        sharedNameToField.values().forEach(field ->
        {
            if (field.isEnum())
            {
                final List<Value> fieldValues = field.values();

                // Rename collisions by name
                final Map<String, Map<String, Long>> nameToReprToCount = fieldValues.stream().collect(
                    groupingBy(Value::description,
                    groupingBy(Value::representation, Collectors.counting())));

                // Make a new unique Value for every input with the name collisions fixed
                final List<Value> values = nameToReprToCount
                    .entrySet().stream().flatMap(e ->
                    {
                        final String name = e.getKey();
                        final Map<String, Long> reprToCount = e.getValue();
                        final String commonRepr = findCommonName(reprToCount);
                        final Stream<Value> commonValues =
                            LongStream.range(0, reprToCount.get(commonRepr)).mapToObj(i ->
                            new Value(commonRepr, name));

                        final HashSet<String> otherReprs = new HashSet<>(reprToCount.keySet());
                        otherReprs.remove(commonRepr);

                        final Stream<Value> otherValues = otherReprs.stream().flatMap(repr ->
                        {
                            final String newName = name + "_" + repr;
                            return LongStream.range(0, reprToCount.get(repr))
                                .mapToObj(i -> new Value(repr, newName));
                        });

                        return Stream.concat(commonValues, otherValues);
                    })
                    .collect(Collectors.toList());

                // Add name collisions by representation to Javadoc
                final Map<String, Map<String, Long>> reprToNameToCount = values.stream().collect(
                    groupingBy(Value::representation,
                    groupingBy(Value::description, Collectors.counting())));

                final List<Value> finalValues = reprToNameToCount
                    .entrySet().stream().map(e ->
                    {
                        final String repr = e.getKey();
                        final Map<String, Long> nameToCount = e.getValue();
                        final String commonName = findCommonName(nameToCount);
                        final Value value = new Value(repr, commonName);
                        if (nameToCount.size() > 1)
                        {
                            final Set<String> otherNames = nameToCount.keySet();
                            otherNames.remove(commonName);
                            value.alternativeNames(new ArrayList<>(otherNames));
                        }
                        return value;
                    })
                    .collect(Collectors.toList());

                fieldValues.clear();
                fieldValues.addAll(finalValues);
            }
        });
    }

    private String findCommonName(final Map<String, Long> nameToCount)
    {
        return nameToCount.keySet().stream().max(Comparator.comparingLong(name -> nameToCount.get(name))).get();
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
            }
            else
            {
                // TODO: check collisions
                // merge enum values
                sharedField.values().addAll(field.values());
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

    private Component findSharedComponent(final Function<Dictionary, Component> getter)
    {
        final Dictionary firstDictionary = inputDictionaries.get(0);
        final Component sharedComponent = copyOf(getter.apply(firstDictionary));
        inputDictionaries.forEach(dict ->
        {
            final Component component = getter.apply(dict);
            mergeAggregate(component, sharedComponent);
        });

        inputDictionaries.forEach(dict ->
        {
            final Component component = getter.apply(dict);
            identifyAggregateEntriesInParent(component, sharedComponent);
        });

        return sharedComponent;
    }

    private void mergeAggregate(final Aggregate aggregate, final Aggregate sharedAggregate)
    {
//        System.out.println("aggregate = " + aggregate);
        aggregate.isInParent(true);

        final Map<String, Entry> nameToEntry = nameToEntry(aggregate.entries());
        final Iterator<Entry> it = sharedAggregate.entries().iterator();
        while (it.hasNext())
        {
            final Entry sharedEntry = it.next();
            final Entry entry = nameToEntry.get(sharedEntry.name());
            if (entry == null || !sharedEntry.isField())
            {
                it.remove();
            }
            else
            {
                // TODO: check collisions
                // Only required if all are required
                sharedEntry.required(sharedEntry.required() && entry.required());
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

    // pre: shared fields calculated
    private Entry copyOf(final Entry entry)
    {
        Entry.Element element = entry.element();

        // Remap fields to calculated shared values
        if (element instanceof Field)
        {
            final Field field = (Field)element;
            final String name = field.name();
            element = sharedNameToField.get(name);
            if (element == null)
            {
                return null;
            }
        }

        return new Entry(entry.required(), element);
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
            final Entry newEntry = copyOf(entry);
            if (newEntry != null)
            {
                newAggregate.entries().add(newEntry);
            }
        }
    }

}
