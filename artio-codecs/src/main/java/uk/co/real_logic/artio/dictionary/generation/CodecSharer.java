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
import uk.co.real_logic.artio.dictionary.ir.*;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

class CodecSharer
{
    private final List<Dictionary> inputDictionaries;

    CodecSharer(final List<Dictionary> inputDictionaries)
    {
        this.inputDictionaries = inputDictionaries;
    }

    public void share()
    {
        final Dictionary firstDictionary = inputDictionaries.get(0);

        final Set<String> commonMessageNames = findCommonMessages();

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
                        nameToMessage.put(name, Message.copyOf(msg));
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
                            mergeAggregate(msg, sharedMessage);
                        }
                    }
                }
            });
        }

        final Map<String, Field> fields = new HashMap<>();
        final Map<String, Component> components = new HashMap<>();
        final Component header = sharedComponent(Dictionary::header);
        final Component trailer = sharedComponent(Dictionary::trailer);
        final String specType = DictionaryParser.DEFAULT_SPEC_TYPE;
        final int majorVersion = 0;
        final int minorVersion = 0;

        // TODO: find all common fields

        final List<Message> messages = new ArrayList<>(nameToMessage.values());
        fields.putAll(firstDictionary.fields());
        components.putAll(firstDictionary.components());

        final Dictionary sharedDictionary = new Dictionary(
            messages,
            fields,
            components,
            header,
            trailer,
            specType,
            majorVersion,
            minorVersion);

        sharedDictionary.shared(true);
        inputDictionaries.forEach(dict -> dict.sharedParent(sharedDictionary));
        inputDictionaries.add(sharedDictionary);
    }

    private Component sharedComponent(final Function<Dictionary, Component> getter)
    {
        final Dictionary firstDictionary = inputDictionaries.get(0);
        final Component sharedComponent = Component.copyOf(getter.apply(firstDictionary));
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

    private Set<String> findCommonMessages()
    {
        final Set<String> messageNames = new HashSet<>();
        inputDictionaries.forEach(dict ->
        {
            final Set<String> namesInDictionary = messageNames(dict);
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

    private Map<String, Entry> nameToEntry(final List<Entry> entries)
    {
        return entries.stream().collect(Collectors.toMap(Entry::name, x -> x));
    }
}
