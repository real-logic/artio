/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.validation;

/**
 * Interface to notify the gateway whether a FIXP acceptor session should be authenticated or not.
 *
 * Either only call the <code>accept </code>or the <code>reject()</code> method. See
 *  * {@link AbstractAuthenticationProxy} for more options to invoke.
 */
public interface FixPAuthenticationProxy extends AbstractAuthenticationProxy
{
}
