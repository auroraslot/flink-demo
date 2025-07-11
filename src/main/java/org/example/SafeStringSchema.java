package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

@Slf4j
public class SafeStringSchema implements DeserializationSchema<String> {
    @Override
    public String deserialize(byte[] message) {
        if (message == null) {
            log.error("message is null");
            return null; // 或返回空字符串 ""
        }
        return new String(message, StandardCharsets.UTF_8);
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
