package se.fnord.log4j2.logstash;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.layout.AbstractLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.StringBuilderEncoder;
import org.apache.logging.log4j.core.net.Severity;
import org.apache.logging.log4j.core.pattern.DatePatternConverter;
import org.apache.logging.log4j.core.util.JsonUtils;
import org.apache.logging.log4j.core.util.NetUtils;
import org.apache.logging.log4j.core.util.StringBuilderWriter;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;
import org.apache.logging.log4j.util.StringBuilders;
import org.apache.logging.log4j.util.Strings;
import se.fnord.taggedmessage.TagConsumer;
import se.fnord.taggedmessage.TaggedMessage;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

@Plugin(name = "LogstashLayoutV1", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE)
public class LogstashLayoutV1 extends AbstractLayout<String> implements StringLayout, TagConsumer<StringBuilder> {
    private static final char C = ',';
    private static final char Q = '\"';
    private static final String QC = "\",";
    private static final String CQ = ",\"";
    private static final String CQU = ",\"_";

    private static final int DEFAULT_STRING_BUILDER_SIZE = 1024;

    private static final int MAX_STRING_BUILDER_SIZE = Math.max(DEFAULT_STRING_BUILDER_SIZE,
            Integer.getInteger("log4j.layoutStringBuilder.maxSize",2 * 1024));

    private static final ThreadLocal<StringBuilder[]> stringBuilders = ThreadLocal.withInitial(
            () -> new StringBuilder[] {
                new StringBuilder(DEFAULT_STRING_BUILDER_SIZE),
                new StringBuilder(DEFAULT_STRING_BUILDER_SIZE)
            });

    private static DatePatternConverter DATE_FORMATTER = DatePatternConverter.newInstance(
            new String[]{"ISO8601_PERIOD", "UTC"});

    private final boolean includeStacktrace;
    private final boolean includeThreadContext;
    private final boolean includeTimestamp;
    private final String objectHeader;
    private final StringBuilderEncoder encoder;

    public static class Builder<B extends Builder<B>> extends AbstractLayout.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<LogstashLayoutV1> {

        @PluginBuilderAttribute
        private String host;

        @PluginBuilderAttribute
        private boolean includeStacktrace = true;

        @PluginBuilderAttribute
        private boolean includeThreadContext = true;

        @PluginBuilderAttribute
        private boolean includeTimestamp = true;

        public Builder() {
            super();
        }

        @Override
        public LogstashLayoutV1 build() {
            return new LogstashLayoutV1(getConfiguration(),
                    host != null ? host : NetUtils.getLocalHostname(),
                    includeStacktrace, includeThreadContext, includeTimestamp);
        }

        public String getHost() {
            return host;
        }

        public boolean isIncludeStacktrace() {
            return includeStacktrace;
        }

        public boolean isIncludeThreadContext() {
            return includeThreadContext;
        }

        public boolean isIncludeTimestamp() {
            return includeTimestamp;
        }

        public B setHost(String host) {
            this.host = host;
            return asBuilder();
        }

        public B setIncludeStacktrace(boolean includeStacktrace) {
            this.includeStacktrace = includeStacktrace;
            return asBuilder();
        }

        public B setIncludeThreadContext(boolean includeThreadContext) {
            this.includeThreadContext = includeThreadContext;
            return asBuilder();
        }

        public B setIncludeTimestamp(boolean includeTimestamp) {
            this.includeTimestamp = includeTimestamp;
            return asBuilder();
        }
    }

    private LogstashLayoutV1(Configuration config, String host, boolean includeStacktrace, boolean includeThreadContext, boolean includeTimestamp) {
        super(config, null, null);
        this.objectHeader = renderObjectHeader(1, host);
        this.includeStacktrace = includeStacktrace;
        this.includeThreadContext = includeThreadContext;
        this.includeTimestamp = includeTimestamp;
        this.encoder = new StringBuilderEncoder(UTF_8);
    }

    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }

    @Override
    public Map<String, String> getContentFormat() {
        return Collections.emptyMap();
    }

    @Override
    public String getContentType() {
        return "application/json";
    }

    @FunctionalInterface
    private interface EventTransformer<T> {
        T transform(LogEvent event, StringBuilder textBuilder, StringBuilder jsonBuilder);
    }

    @FunctionalInterface
    private interface EventTransformerTo<T> {
        void transformTo(LogEvent event, T into, StringBuilder textBuilder, StringBuilder jsonBuilder);
    }

    private static <T> T transformEvent(LogEvent event, EventTransformer<T> transformer) {
        StringBuilder builders[] = stringBuilders.get();
        builders[0].setLength(0);
        builders[1].setLength(0);
        try {
            return transformer.transform(event, builders[0], builders[1]);
        }
        finally {
            StringBuilders.trimToMaxSize(builders[0], MAX_STRING_BUILDER_SIZE);
            StringBuilders.trimToMaxSize(builders[1], MAX_STRING_BUILDER_SIZE);
        }
    }


    @Override
    public Charset getCharset() {
        return UTF_8;
    }

    private static <T> void transformEvent(LogEvent event, T into, EventTransformerTo<T> transformer) {
        StringBuilder builders[] = stringBuilders.get();
        builders[0].setLength(0);
        builders[1].setLength(0);
        try {
            transformer.transformTo(event, into, builders[0], builders[1]);
        }
        finally {
            StringBuilders.trimToMaxSize(builders[0], MAX_STRING_BUILDER_SIZE);
            StringBuilders.trimToMaxSize(builders[1], MAX_STRING_BUILDER_SIZE);
        }
    }

    private byte[] toByteArray(LogEvent event, StringBuilder textBuilder, StringBuilder jsonBuilder) {
        toText(event, textBuilder, jsonBuilder);
        return jsonBuilder.toString().getBytes(UTF_8);
    }

    @Override
    public byte[] toByteArray(LogEvent event) {
        return transformEvent(event, this::toByteArray);
    }


    private void encode(LogEvent event, ByteBufferDestination destination, StringBuilder textBuilder, StringBuilder jsonBuilder) {
        toText(event, textBuilder, jsonBuilder);
        encoder.encode(jsonBuilder, destination);
    }

    @Override
    public void encode(LogEvent event, ByteBufferDestination destination) {
        transformEvent(event, destination, this::encode);
    }

    private String toSerializable(LogEvent event, StringBuilder textBuilder, StringBuilder jsonBuilder) {
        toText(event, textBuilder, jsonBuilder);
        return jsonBuilder.toString();
    }

    @Override
    public String toSerializable(LogEvent event) {
        return transformEvent(event, this::toSerializable);
    }

    private static String renderObjectHeader(int version, String host) {
        StringBuilder builder = new StringBuilder();
        appendVersionField(version, builder);
        appendHostField(host, builder);
        return builder.toString();
    }

    private void toText(LogEvent event, StringBuilder textBuilder, StringBuilder jsonBuilder) {
        jsonBuilder.append('{');
        jsonBuilder.append(objectHeader);
        if (includeTimestamp) {
            jsonBuilder.append("\"@timestamp\":\"");
            appendTimestamp(event.getTimeMillis(), jsonBuilder)
                    .append(QC);
        }
        jsonBuilder.append("\"level\":\"")
                .append(event.getLevel().name())
                .append(QC);
        jsonBuilder.append("\"level_value\":");
        appendLevelValue(event.getLevel(), jsonBuilder);

        if (event.getThreadName() != null) {
            jsonBuilder.append(",\"thread_name\":\"");
            JsonUtils.quoteAsString(event.getThreadName(), jsonBuilder);
            jsonBuilder.append(Q);
        }

        if (event.getLoggerName() != null) {
            jsonBuilder.append(",\"logger_name\":\"");
            JsonUtils.quoteAsString(event.getLoggerName(), jsonBuilder);
            jsonBuilder.append(Q);
        }

        if (includeThreadContext) {
            event.getContextData()
                    .forEach(LogstashLayoutV1::appendKeyValue, jsonBuilder);
        }

        if (includeStacktrace && event.getThrown() != null) {
            jsonBuilder.append(",\"stack_trace\":\"");
            appendThrowable(event.getThrown(), textBuilder, jsonBuilder);
            jsonBuilder.append(Q);
        }

        Message message = event.getMessage();
        if (message instanceof TaggedMessage) {
            ((TaggedMessage) message).getTags().forEach(jsonBuilder, this);
        }
        else {
            jsonBuilder.append(",\"message\":\"");
            appendMessage(message, textBuilder, jsonBuilder);
            jsonBuilder.append(Q);
        }
        jsonBuilder.append('}');
        jsonBuilder.append('\n');
    }

    private static CharSequence toNullSafeString(CharSequence s) {
        return s == null ? Strings.EMPTY : s;
    }

    static void appendVersionField(int version, StringBuilder jsonBuilder) {
        jsonBuilder.append("\"@version\":")
                .append(version)
                .append(C);
    }

    static void appendHostField(String host, StringBuilder jsonBuilder) {
        jsonBuilder.append("\"source_host\":\"");
        JsonUtils.quoteAsString(toNullSafeString(host), jsonBuilder);
        jsonBuilder.append(QC);
    }

    static void appendMessage(Message message, StringBuilder textBuilder, StringBuilder jsonBuilder) {
        if (message instanceof CharSequence) {
            JsonUtils.quoteAsString((CharSequence) message, jsonBuilder);
        } else if (message instanceof StringBuilderFormattable) {
            textBuilder.setLength(0);
            ((StringBuilderFormattable) message).formatTo(textBuilder);
            JsonUtils.quoteAsString(textBuilder, jsonBuilder);
        } else {
            JsonUtils.quoteAsString(toNullSafeString(message.getFormattedMessage()), jsonBuilder);
        }
    }

    static void appendKeyValue(CharSequence key, Object value, StringBuilder stringBuilder) {
        stringBuilder.append(CQU);
        JsonUtils.quoteAsString(key, stringBuilder);
        stringBuilder.append("\":\"");
        JsonUtils.quoteAsString(toNullSafeString(String.valueOf(value)), stringBuilder);
        stringBuilder.append(Q);
    }

    static void appendTaggedTextValue(CharSequence key, Object value, StringBuilder stringBuilder) {
        stringBuilder.append(CQ);
        JsonUtils.quoteAsString(key, stringBuilder);
        stringBuilder.append("\":");
        stringBuilder.append(Q);
        JsonUtils.quoteAsString(toNullSafeString(String.valueOf(value)), stringBuilder);
        stringBuilder.append(Q);
    }

    static void appendTaggedLongValue(CharSequence key, long value, StringBuilder stringBuilder) {
        stringBuilder.append(CQ);
        JsonUtils.quoteAsString(key, stringBuilder);
        stringBuilder.append("\":");
        stringBuilder.append(value);
    }

    static void appendTaggedDoubleValue(CharSequence key, double value, StringBuilder stringBuilder) {
        stringBuilder.append(CQ);
        JsonUtils.quoteAsString(key, stringBuilder);
        stringBuilder.append("\":");
        stringBuilder.append(value);
    }

    static void appendTaggedBooleanValue(CharSequence key, boolean value, StringBuilder stringBuilder) {
        stringBuilder.append(CQ);
        JsonUtils.quoteAsString(key, stringBuilder);
        stringBuilder.append("\":");
        stringBuilder.append(value);
    }

    static void appendTaggedNullValue(CharSequence key, StringBuilder stringBuilder) {
        stringBuilder.append(CQ);
        JsonUtils.quoteAsString(key, stringBuilder);
        stringBuilder.append("\":null");
    }

    static StringBuilder appendTimestamp(long timeMillis, StringBuilder stringBuilder) {
        DATE_FORMATTER.format(timeMillis, stringBuilder);
        stringBuilder.append('Z'); // Always UTC
        return stringBuilder;
    }

    static StringBuilder appendLevelValue(Level level, StringBuilder stringBuilder) {
        int levelValue = Severity.getSeverity(level).getCode();
        stringBuilder.append(levelValue);
        return stringBuilder;
    }

    static void appendThrowable(Throwable throwable, StringBuilder textBuilder, StringBuilder jsonBuilder) {
        textBuilder.setLength(0);
        StringBuilderWriter sw = new StringBuilderWriter(textBuilder);
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        JsonUtils.quoteAsString(textBuilder, jsonBuilder);
    }

    @Override
    public void textTag(CharSequence key, CharSequence value, StringBuilder stringBuilder) {
        LogstashLayoutV1.appendTaggedTextValue(key, value, stringBuilder);
    }

    @Override
    public void longTag(CharSequence key, long value, StringBuilder stringBuilder) {
        LogstashLayoutV1.appendTaggedLongValue(key, value, stringBuilder);
    }

    @Override
    public void booleanTag(CharSequence key, boolean value, StringBuilder stringBuilder) {
        LogstashLayoutV1.appendTaggedBooleanValue(key, value, stringBuilder);
    }

    @Override
    public void doubleTag(CharSequence key, double value, StringBuilder stringBuilder) {
        LogstashLayoutV1.appendTaggedDoubleValue(key, value, stringBuilder);
    }

    @Override
    public void nullTag(CharSequence key, StringBuilder stringBuilder) {
        LogstashLayoutV1.appendTaggedNullValue(key, stringBuilder);
    }
}