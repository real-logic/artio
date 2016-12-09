/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway;

import java.util.HashMap;
import java.util.Map;

public final class CloseChecker
{
    private static final boolean CLOSE_CHECKER_ENABLED = Boolean.getBoolean("fix.core.close_checker");

    private static final Map<String, Resource> RESOURCES = new HashMap<>();

    public static void onOpen(final String resourceId, final String ownerId)
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

                resource.currentlyOpen.put(ownerId, e);
            }
        }
    }

    public static void onClose(final String resourceId, final String ownerId)
    {
        if (CLOSE_CHECKER_ENABLED)
        {
            final Resource resource = RESOURCES.get(resourceId);
            if (resource != null)
            {
                resource.currentlyOpen.remove(ownerId);
            }
        }
    }

    public static void validate(final String resourceId)
    {
        if (CLOSE_CHECKER_ENABLED)
        {
            final Resource resource = RESOURCES.get(resourceId);
            if (resource != null && !resource.currentlyOpen.isEmpty())
            {
                final IllegalStateException exception = new IllegalStateException(String.format(
                    "Resource [%s] open by %s",
                    resourceId,
                    resource.currentlyOpen.keySet()));

                resource.currentlyOpen.values().forEach(exception::addSuppressed);

                throw exception;
            }
        }
    }

    private static final class Resource
    {
        final Map<String, Exception> currentlyOpen = new HashMap<>();

        public void onOpen(final String ownerId, final Exception e)
        {
            currentlyOpen.put(ownerId, e);
        }
    }
}
