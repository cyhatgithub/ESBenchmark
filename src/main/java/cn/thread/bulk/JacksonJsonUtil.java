package cn.thread.bulk;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * To compile and test manually.
 *
 * yi@yi-ThinkPad-T430:~/viz/cloudmon/alert-common$ mvn package assembly:single
 * yi@yi-ThinkPad-T430:~/viz/cloudmon/alert-common$ java -cp target/cloudmon-alert-common-1.0-SNAPSHOT-jar-with-dependencies.jar com.cloudmon.alerting.common.JacksonJsonUtil
 *
 */
public class JacksonJsonUtil {
    public static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
    }

    public static String toJson(Object obj) throws JsonProcessingException {
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }

    public static String toJsonCompact(Object obj) throws JsonProcessingException {
        return objectMapper.writer((PrettyPrinter) null).writeValueAsString(obj);
    }

    public static String toJsonCompact(String str) throws JsonProcessingException {
        try {
            Map map = objectMapper.readValue(str, Map.class);
            return toJsonCompact(map);
        }
        catch (IOException ioex) {
            return str;
        }
    }

    public static<T> T fromJson(String s, Class<T> tClass) throws IOException {
        return objectMapper.readValue(s, tClass);
    }

    public static<T> T fromJson(String jsonString, JavaType javaType) throws IOException {
        return objectMapper.readValue(jsonString, javaType);
    }

    public static<T> List<T> listFromJson(String s) throws IOException {
        return objectMapper.readValue(s, new TypeReference<List<T>>(){});
    }

    public static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
        return objectMapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }
}
