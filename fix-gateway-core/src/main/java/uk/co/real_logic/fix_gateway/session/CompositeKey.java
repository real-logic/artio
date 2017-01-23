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
package uk.co.real_logic.fix_gateway.session;

/**
 * The full identifying key of the session in question. Methods may return
 * empty Strings if the {@link SessionIdStrategy} doesn't use that information
 * as part of session identification.
 *
 * Implementors should provide appropriate equals/hashcode methods.
 */
public interface CompositeKey
{
    String localCompId();

    String localSubId();

    String localLocationId();

    String remoteCompId();

    String remoteSubId();

    String remoteLocationId();
}
