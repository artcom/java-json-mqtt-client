package de.artcom.mqtt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class SubscriptionHandlerTest {
    private SubscriptionHandler handler;
    private IMqttAsyncClient mockClient;
    private MessageCallback callback1;
    private MessageCallback callback2;

    private static final String topic = "testTopic";

    @Before
    public void setup() {
        mockClient = mock(IMqttAsyncClient.class);
        when(mockClient.isConnected()).thenReturn(true);

        handler = new SubscriptionHandler(mockClient);

        callback1 = mock(MessageCallback.class);
        callback2 = mock(MessageCallback.class);
    }

    @Test
    public void subscribesToTopic() throws MqttException {
        handler.subscribe(topic, callback1);
        verify(mockClient, times(1)).subscribe(topic, 2);
        assertEquals(1, handler.getCallbacks(topic).size());
    }

    @Test
    public void subscribeOnceToTopic() throws MqttException {
        handler.subscribe(topic, callback1);
        handler.subscribe(topic, callback2);
        verify(mockClient, times(1)).subscribe(topic, 2);
        assertEquals(2, handler.getCallbacks(topic).size());
    }

    @Test
    public void unsubscribesFromTopic() throws MqttException {
        handler.subscribe(topic, callback1);
        handler.unsubscribe(topic, callback1);
        verify(mockClient, times(1)).unsubscribe(topic);
        assertEquals(0, handler.getCallbacks(topic).size());
    }

    @Test
    public void unsubscribeOnceFromTopic() throws MqttException {
        handler.subscribe(topic, callback1);
        handler.subscribe(topic, callback2);
        handler.unsubscribe(topic, callback1);
        handler.unsubscribe(topic, callback2);

        handler.unsubscribe(topic, callback2);
        verify(mockClient, times(1)).unsubscribe(topic);
        assertEquals(0, handler.getCallbacks(topic).size());
    }

    static class PayloadClass {
        public String foo;
        public int bar;

        @JsonCreator
        PayloadClass(@JsonProperty("foo") String foo, @JsonProperty("bar") int bar) {
            this.foo = foo;
            this.bar = bar;
        }
    }

    @Test
    public void parseJSONPayload() throws MqttException, IOException {
        final PayloadClass expected = new PayloadClass("foo", 1234);

        final PayloadClass[] actual = {null};
        handler.subscribe(topic, new MessageCallback<PayloadClass>() {
            @Override
            public void onMessage(String topic, PayloadClass payload, MqttMessage message) {
                actual[0] = payload;
            }
        });

        List<IMessageCallback> callbacks = handler.getCallbacks(topic);
        assertEquals(callbacks.size(), 1);

        callbacks.get(0).onRawMessage(topic, new MqttMessage(new ObjectMapper().writeValueAsBytes(expected)));
        assertEquals(expected.foo, actual[0].foo);
        assertEquals(expected.bar, actual[0].bar);
    }

    @Test
    public void errorOnInvalidJSON() throws MqttException {
        final Boolean[] wasError = {false};
        handler.subscribe(topic, new MessageCallback<String>() {
            @Override
            public void onMessage(String topic, String payload, MqttMessage message) {}

            @Override
            public void onParseError(IOException error, MqttMessage message) {
                wasError[0] = true;
            }
        });

        List<IMessageCallback> callbacks = handler.getCallbacks(topic);
        assertEquals(callbacks.size(), 1);

        callbacks.get(0).onRawMessage(topic, new MqttMessage("invalid".getBytes()));
        assertEquals(true, wasError[0]);
    }

    @Test
    public void emptyMessage() throws MqttException {
        final Boolean[] wasEmpty = {false};
        handler.subscribe(topic, new MessageCallback<String>() {
            @Override
            public void onMessage(String topic, String payload, MqttMessage message) {}

            @Override
            public void onEmptyMessage(String topic, MqttMessage message) {
                wasEmpty[0] = true;
            }
        });

        List<IMessageCallback> callbacks = handler.getCallbacks(topic);
        assertEquals(callbacks.size(), 1);

        callbacks.get(0).onRawMessage(topic, new MqttMessage(new byte[]{}));
        assertEquals(true, wasEmpty[0]);
    }

    @Test
    public void rawMessage() throws MqttException {
        String expected = "message";

        final String[] actual = {null};
        handler.subscribe(topic, new MessageCallback() {
            @Override
            public void onRawMessage(String topic, MqttMessage message) {
                actual[0] = message.toString();
            }
        });

        List<IMessageCallback> callbacks = handler.getCallbacks(topic);
        assertEquals(callbacks.size(), 1);

        callbacks.get(0).onRawMessage(topic, new MqttMessage(expected.getBytes()));
        assertEquals(expected, actual[0]);
    }
}
