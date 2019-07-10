# Logstash json event layout for Log4J2

[![Build Status](https://travis-ci.org/gsson/log4j2-logstash-layout.svg)](https://travis-ci.org/gsson/log4j2-logstash-layout)

The goal is to be reasonably performant and (near) garbage-free.

The layout lacks many options, but the defaults should produce sane messages conforming to the Logback [LoggingEvent fields](https://github.com/logstash/logstash-logback-encoder#loggingevent-fields). Each message is separated by a newline. 

The layout is modelled after [GelfLayout](https://github.com/apache/logging-log4j2/blob/master/log4j-core/src/main/java/org/apache/logging/log4j/core/layout/GelfLayout.java).

## Usage

For printing to standard out in a format suitable for eg. [fluentd](https://www.fluentd.org/):
```xml
<Appenders>
    <Console name="console" target="SYSTEM_OUT">
        <LogstashLayoutV1 host="someserver" includeStacktrace="true" includeThreadContext="false"/>
    </Console>
</Appenders>
```

Supported parameters are:
* `host`: The hostname, appears in the `source_host` field. Optional, defaults to the local hostname.
* `includeStacktrace`: Enable logging of stacktraces of logged exceptions. Optional, defaults to `true`.
* `includeThreadContext`: Enable logging of the thread context. Optional, defaults to `true`.
* `includeTimestamp`: Enable logging of the `@timestamp` field. Optional, defaults to `true`.
