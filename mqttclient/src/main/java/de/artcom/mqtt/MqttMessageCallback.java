package de.artcom.mqtt;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.logging.Logger;

public abstract class MqttMessageCallback<T> implements IMqttMessageCallback {
    private static final Logger LOG = Logger.getLogger(MqttClient.class.getSimpleName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    protected MqttMessageCallback() {
        objectMapper.setVisibility(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.NONE)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));
    }

    public void handleMessage(String topic, MqttMessage message) {
        if (message.getPayload().length == 0) {
            onEmptyMessage(topic, message);
        } else {
            try {
                // retrieve the runtime class of T
                Type arg = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
                T payload = objectMapper.readValue(message.getPayload(), objectMapper.getTypeFactory().constructType(arg));
                onMessage(topic, payload, message);
            } catch (IOException error) {
                onParseError(error, message);
            }
        }
    }

    public abstract void onMessage(String topic, T payload, MqttMessage message);

    public void onParseError(IOException error, MqttMessage message) {
        LOG.severe("Error deserializing payload '" + message + "': " + error.getMessage());
    }

    public void onEmptyMessage(String topic, MqttMessage message) {
        LOG.info("Empty message for topic '" + topic + "' ignored");
    }
}
