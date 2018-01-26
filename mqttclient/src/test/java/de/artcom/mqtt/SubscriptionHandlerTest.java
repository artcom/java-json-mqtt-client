package de.artcom.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class SubscriptionHandlerTest {

    private SubscriptionHandler handler;
    private IMqttAsyncClient mockClient;
    private MessageCallback callback1;
    private MessageCallback callback2;

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
        handler.subscribe("testTopic", callback1);
        verify(mockClient, times(1)).subscribe("testTopic", 2);
        assertEquals(1, handler.getCallbacks("testTopic").size());
    }

    @Test
    public void subscribeOnceToTopic() throws MqttException {
        handler.subscribe("testTopic", callback1);
        handler.subscribe("testTopic", callback2);
        verify(mockClient, times(1)).subscribe("testTopic", 2);
        assertEquals(2, handler.getCallbacks("testTopic").size());
    }

    @Test
    public void unsubscribesFromTopic() throws MqttException {
        handler.subscribe("testTopic", callback1);
        handler.unsubscribe("testTopic", callback1);
        verify(mockClient, times(1)).unsubscribe("testTopic");
        assertEquals(0, handler.getCallbacks("testTopic").size());
    }

    @Test
    public void unsubscribeOnceFromTopic() throws MqttException {
        handler.subscribe("testTopic", callback1);
        handler.subscribe("testTopic", callback2);
        handler.unsubscribe("testTopic", callback1);
        handler.unsubscribe("testTopic", callback2);

        handler.unsubscribe("testTopic", callback2);
        verify(mockClient, times(1)).unsubscribe("testTopic");
        assertEquals(0, handler.getCallbacks("testTopic").size());
    }
}
