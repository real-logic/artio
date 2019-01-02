# Artio

[![Join the chat at https://gitter.im/real-logic/artio](https://badges.gitter.im/real-logic/artio.svg)](https://gitter.im/real-logic/artio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
High-Performance FIX Gateway

[![Build status](https://ci.appveyor.com/api/projects/status/js244x1yn26m1nhw/branch/master?svg=true)](https://ci.appveyor.com/project/RichardWarburton/artio/branch/master)

## How to build

The first time that you build the project you need to configure the repository
list for the project. If you're using maven central then you can simple copy the
existing `init.gradle` file into your configuration directory, for example:

```
    cp init.gradle ~/.gradle/
```

If you need a custom binary repository then you can put whatever configuration is
required in your `init.gradle` file. Now you can just run the standard Gradle build
and install process:

```sh
    ./gradlew
```

After the first build this is all that is required.
