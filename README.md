# FIX-Gateway
Resilient High-Performance FIX Gateway

## How to build

The first time that you build the project you need to configure the repository
list for the project. If you're using maven central then you can simple copy the
existing init.gradle file into your configuration directory, for example:

```
cp init.gradle ~/.gradle/
```

If you need a custom binary repository then you can put whatever configuration is
required in your init.gradle file. Now you can just run the standard gradle build
and install process:

```
./gradlew
```

After the first build this is all that is required.
