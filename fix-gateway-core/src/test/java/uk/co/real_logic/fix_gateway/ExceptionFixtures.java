/*
 * Copyright 2015-2017 Real Logic Ltd.
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

public final class ExceptionFixtures
{
    public static Exception fooException;
    public static Exception longerFooException;
    public static Exception nullPointerException;

    static
    {
        try
        {
            bar();
        }
        catch (final Exception e)
        {
            fooException = e;
        }

        try
        {
            someLikeRealllllllyLongMethodName();
        }
        catch (final Exception e)
        {
            longerFooException = e;
        }

        try
        {
            baz();
        }
        catch (final Exception e)
        {
            nullPointerException = e;
        }
    }

    private static void someLikeRealllllllyLongMethodName()
    {
        bar();
    }

    private static void baz()
    {
        ((Object) null).toString();
    }

    private static void bar()
    {
        foo();
    }

    private static void foo()
    {
        throw new RuntimeException();
    }

}
