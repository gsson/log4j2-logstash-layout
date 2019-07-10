package se.fnord.log4j2.logstash;

import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.ObjectArrayMessage;
import org.apache.logging.log4j.message.ObjectMessage;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.junit.jupiter.api.Test;
import se.fnord.taggedmessage.TaggedMessage;
import se.fnord.taggedmessage.Tags;

import java.io.*;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;
import static net.javacrumbs.jsonunit.fluent.JsonFluentAssert.assertThatJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteArrayDestination extends OutputStreamManager {

    public ByteArrayDestination() {
        this(new ByteArrayOutputStream(), "ByteArrayDestination", null, false);
    }

    protected ByteArrayDestination(OutputStream os, String streamName, Layout<?> layout, boolean writeHeader) {
        super(os, streamName, layout, writeHeader);
    }

    public byte[] getBytes() {
        try {
            ByteArrayOutputStream outputStream = (ByteArrayOutputStream) getOutputStream();
            return outputStream.toByteArray();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

public class LogstashLayoutV1Test {
    @Test
    public void testAppendTimestamp() {
        StringBuilder jsonBuilder = new StringBuilder();
        LogstashLayoutV1.appendTimestamp(1, jsonBuilder);
        assertEquals("1970-01-01T00:00:00.001Z", jsonBuilder.toString());
    }

    @Test
    public void contentFormatIsEmpty() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder().build();
        assertTrue(layout.getContentFormat().isEmpty());
    }

    @Test
    public void contentTypeIsJson() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder().build();
        assertEquals("application/json", layout.getContentType());
    }

    @Test
    public void testAppendLevelValue() {
        StringBuilder jsonBuilder = new StringBuilder();
        LogstashLayoutV1.appendLevelValue(Level.FATAL, jsonBuilder);
        assertEquals("1", jsonBuilder.toString());
    }

    @Test
    public void testAppendThrowable() {
        IllegalArgumentException test = new IllegalArgumentException("Test");
        StringBuilder textBuilder = new StringBuilder();
        StringBuilder jsonBuilder = new StringBuilder();
        LogstashLayoutV1.appendThrowable(test, textBuilder, jsonBuilder);
        assertTrue(jsonBuilder.toString().startsWith("java.lang.IllegalArgumentException: Test"));
    }

    private static final Log4jLogEvent LOG_EVENT = Log4jLogEvent.newBuilder()
            .setTimeMillis(1)
            .setThreadName("thread-name")
            .setLoggerName("logger-name")
            .setContextData(new SortedArrayStringMap(Collections.singletonMap("key", "value")))
            .setLevel(Level.DEBUG)
            .setMessage(new SimpleMessage("message"))
            .setThrown(new Exception().fillInStackTrace())
            .build();

    private static String printStackTrace(Throwable t) {
        StringWriter stringWriter = new StringWriter();
        t.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    private static String encode(LogstashLayoutV1 layout, LogEvent event) {
        try (ByteArrayDestination d = new ByteArrayDestination()) {
            layout.encode(event, d);
            d.flush();
            return new String(d.getBytes(), UTF_8);
        }
    }

    private static String toByteArray(LogstashLayoutV1 layout, LogEvent event) {
        return new String(layout.toByteArray(event), UTF_8);
    }

    @Test
    public void rendersCharSequenceMessageWithDefaultSettings() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder()
                .setHost("host-name")
                .build();

        String stackTrace = printStackTrace(LOG_EVENT.getThrown());

        String s = layout.toSerializable(LOG_EVENT);
        assertTrue(s.endsWith("\n"));

        assertThatJson(s)
                .node("@version").isEqualTo(1)
                .node("@timestamp").isEqualTo("1970-01-01T00:00:00.001Z")
                .node("logger_name").isEqualTo("logger-name")
                .node("thread_name").isEqualTo("thread-name")
                .node("source_host").isEqualTo("host-name")
                .node("level").isEqualTo("DEBUG")
                .node("level_value").isEqualTo(7)
                .node("message").isEqualTo("message")
                .node("stack_trace").isEqualTo(TextNode.valueOf(stackTrace))
                .node("_key").isEqualTo("value");

        assertEquals(s, toByteArray(layout, LOG_EVENT));
        assertEquals(s, encode(layout, LOG_EVENT));
    }

    @Test
    public void rendersCharSequenceMessageOmittingStackTraces() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder()
                .setHost("host-name")
                .setIncludeStacktrace(false)
                .build();

        String s = layout.toSerializable(LOG_EVENT);
        assertTrue(s.endsWith("\n"));

        assertThatJson(s)
                .node("@version").isEqualTo(1)
                .node("@timestamp").isEqualTo("1970-01-01T00:00:00.001Z")
                .node("logger_name").isEqualTo("logger-name")
                .node("thread_name").isEqualTo("thread-name")
                .node("source_host").isEqualTo("host-name")
                .node("level").isEqualTo("DEBUG")
                .node("level_value").isEqualTo(7)
                .node("message").isEqualTo("message")
                .node("stack_trace").isAbsent()
                .node("_key").isEqualTo("value");

        assertEquals(s, toByteArray(layout, LOG_EVENT));
        assertEquals(s, encode(layout, LOG_EVENT));
    }

    @Test
    public void rendersCharSequenceMessageOmittingThreadContext() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder()
                .setHost("host-name")
                .setIncludeThreadContext(false)
                .build();
        String stackTrace = printStackTrace(LOG_EVENT.getThrown());

        String s = layout.toSerializable(LOG_EVENT);
        assertTrue(s.endsWith("\n"));

        assertThatJson(s)
                .node("@version").isEqualTo(1)
                .node("@timestamp").isEqualTo("1970-01-01T00:00:00.001Z")
                .node("logger_name").isEqualTo("logger-name")
                .node("thread_name").isEqualTo("thread-name")
                .node("source_host").isEqualTo("host-name")
                .node("level").isEqualTo("DEBUG")
                .node("level_value").isEqualTo(7)
                .node("message").isEqualTo("message")
                .node("stack_trace").isEqualTo(TextNode.valueOf(stackTrace))
                .node("_key").isAbsent();

        assertEquals(s, toByteArray(layout, LOG_EVENT));
        assertEquals(s, encode(layout, LOG_EVENT));
    }

    @Test
    public void rendersCharSequenceMessageOmittingTimestamp() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder()
                .setHost("host-name")
                .setIncludeTimestamp(false)
                .build();
        String stackTrace = printStackTrace(LOG_EVENT.getThrown());

        String s = layout.toSerializable(LOG_EVENT);
        assertTrue(s.endsWith("\n"));

        assertThatJson(s)
                .node("@version").isEqualTo(1)
                .node("@timestamp").isAbsent()
                .node("logger_name").isEqualTo("logger-name")
                .node("thread_name").isEqualTo("thread-name")
                .node("source_host").isEqualTo("host-name")
                .node("level").isEqualTo("DEBUG")
                .node("level_value").isEqualTo(7)
                .node("message").isEqualTo("message")
                .node("stack_trace").isEqualTo(TextNode.valueOf(stackTrace))
                .node("_key").isEqualTo("value");

        assertEquals(s, toByteArray(layout, LOG_EVENT));
        assertEquals(s, encode(layout, LOG_EVENT));
    }

    @Test
    public void renderStringBuilderFormattable() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder()
                .setHost("host-name")
                .build();
        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setTimeMillis(1)
                .setLevel(Level.DEBUG)
                .setMessage(new ObjectMessage("message"))
                .build();

        String s = layout.toSerializable(event);
        assertTrue(s.endsWith("\n"));

        assertThatJson(s)
                .node("@version").isEqualTo(1)
                .node("@timestamp").isEqualTo("1970-01-01T00:00:00.001Z")
                .node("source_host").isEqualTo("host-name")
                .node("level").isEqualTo("DEBUG")
                .node("level_value").isEqualTo(7)
                .node("message").isEqualTo("message");


        assertEquals(s, toByteArray(layout, event));
        assertEquals(s, encode(layout, event));
    }

    @Test
    public void renderNonStringBuilderFormattable() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder()
                .setHost("host-name")
                .build();

        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setTimeMillis(1)
                .setLevel(Level.DEBUG)
                .setMessage(new ObjectArrayMessage("message"))
                .build();

        String s = layout.toSerializable(event);
        assertTrue(s.endsWith("\n"));

        assertThatJson(s)
                .node("@version").isEqualTo(1)
                .node("@timestamp").isEqualTo("1970-01-01T00:00:00.001Z")
                .node("source_host").isEqualTo("host-name")
                .node("level").isEqualTo("DEBUG")
                .node("level_value").isEqualTo(7)
                .node("message").isEqualTo(TextNode.valueOf("[message]"));

        assertEquals(s, toByteArray(layout, event));
        assertEquals(s, encode(layout, event));
    }

    @Test
    public void rendersTaggedMessage() {
        LogstashLayoutV1 layout = LogstashLayoutV1.newBuilder()
                .setHost("host-name")
                .build();

        Log4jLogEvent event = Log4jLogEvent.newBuilder()
                .setTimeMillis(1)
                .setLevel(Level.DEBUG)
                .setMessage(new TaggedMessage(Tags.of(
                        "message", "message",
                        "number", 42,
                        "boolean", true), null))
                .build();

        String s = layout.toSerializable(event);
        assertTrue(s.endsWith("\n"));

        assertThatJson(s)
                .node("@version").isEqualTo(1)
                .node("@timestamp").isEqualTo("1970-01-01T00:00:00.001Z")
                .node("source_host").isEqualTo("host-name")
                .node("level").isEqualTo("DEBUG")
                .node("level_value").isEqualTo(7)
                .node("message").isEqualTo("message")
                .node("number").isEqualTo(42)
                .node("boolean").isEqualTo(true);

        assertEquals(s, toByteArray(layout, event));
        assertEquals(s, encode(layout, event));
    }
}
