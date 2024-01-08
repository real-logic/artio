/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio;

import java.util.HashMap;
import java.util.Map;

public final class CloseChecker
{
    private static final boolean CLOSE_CHECKER_ENABLED = Boolean.getBoolean("fix.core.close_checker");

    private static final Map<String, Resource> RESOURCES = new HashMap<>();

    public static synchronized void onOpen(final String resourceId, final Object owner)
    {
        if (CLOSE_CHECKER_ENABLED)
        {
            try
            {
                throw new Exception();
            }
            catch (final Exception e)
            {
                final Resource resource = RESOURCES.computeIfAbsent(
                    resourceId, (key) -> new Resource());

                final Exception oldException = resource.currentlyOpen.put(owner, e);
                if (oldException != null)
                {
                    final Error error = error(resourceId, owner);
                    error.addSuppressed(oldException);
                    throw error;
                }
            }
        }
    }

    public static synchronized void onClose(final String resourceId, final Object owner)
    {
        if (CLOSE_CHECKER_ENABLED)
        {
            final Resource resource = RESOURCES.get(resourceId);
            if (resource != null)
            {
                resource.currentlyOpen.remove(owner);
            }
        }
    }

    public static synchronized void validateAll()
    {
        if (CLOSE_CHECKER_ENABLED)
        {
            RESOURCES.keySet().forEach(CloseChecker::validate);
        }
    }

    public static synchronized void validate(final String resourceId)
    {
        if (CLOSE_CHECKER_ENABLED)
        {
            final Resource resource = RESOURCES.get(resourceId);
            if (resource != null && !resource.isEmpty())
            {
                final Error error = error(resourceId, resource.currentlyOpen.keySet());
                resource.currentlyOpen.values().forEach(error::addSuppressed);
                throw error;
            }
        }
    }

    private static Error error(final String resourceId, final Object owned)
    {
        return new Error(String.format(
            "Resource [%s] open by %s [%s]",
            resourceId,
            owned,
            owned.getClass()));
    }

    private static final class Resource
    {
        final Map<Object, Exception> currentlyOpen = new HashMap<>();

        private boolean isEmpty()
        {
            // Work around potential JDK 9 bug
            return currentlyOpen.size() <= 0;
        }
    }
}
