package de.artcom.mqtt;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Client implements MqttCallback, IMqttActionListener {
    private static final Logger LOG = Logger.getLogger(Client.class.getSimpleName());

    private static final int MAX_RECONNECT_DELAY = 10000;
    private static final int DEFAULT_KEEPALIVE_SECONDS = 2;

    private final MqttConnectOptions connectOptions;
    private final ConnectionCallback connectionCallback;
    private final MqttAsyncClient pahoClient;
    private final SubscriptionHandler subscriptionHandler;

    private final ScheduledExecutorService connectScheduler;
    private int connectDelay;
    private ScheduledFuture<?> connectingFuture;
    private boolean isConnecting;
    private boolean hasConnected;
    private final ObjectMapper objectMapper;

    public Client(String tcpBrokerUri, String mqttClientId, ConnectionCallback callback) throws MqttException {
        this(tcpBrokerUri, mqttClientId, callback, DEFAULT_KEEPALIVE_SECONDS);
    }

    public Client(String tcpBrokerUri, String mqttClientId, ConnectionCallback callback, int keepAlive) throws MqttException {
        this(tcpBrokerUri, mqttClientId, callback, keepAlive, true);
    }

    public Client(String tcpBrokerUri, String mqttClientId, ConnectionCallback callback, int keepAlive, boolean cleanSession) throws MqttException {
        connectionCallback = callback;
        connectOptions = new MqttConnectOptions();
        connectOptions.setKeepAliveInterval(keepAlive);
        connectOptions.setConnectionTimeout(5);
        connectOptions.setCleanSession(cleanSession);

        connectScheduler = Executors.newSingleThreadScheduledExecutor();

        pahoClient = new MqttAsyncClient(tcpBrokerUri, mqttClientId, new MemoryPersistence(), new TimerPingSender());
        pahoClient.setCallback(this);

        subscriptionHandler = new SubscriptionHandler(pahoClient);

        objectMapper = new ObjectMapper();
        objectMapper.setVisibility(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
    }

    public void connect() {
        scheduleConnect(0);
    }

    synchronized private void scheduleConnect(int delay) {
        if (pahoClient.isConnected()) {
            return;
        }

        if (connectingFuture != null) {
            if (connectingFuture.getDelay(TimeUnit.MILLISECONDS) <= delay) {
                return;
            } else {
                connectingFuture.cancel(false);
            }
        }

        if (!isConnecting) {
            LOG.info("Scheduling connect in " + connectDelay + " milliseconds");
            connectingFuture = connectScheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        isConnecting = true;
                        connectingFuture = null;
                        pahoClient.connect(connectOptions, null, Client.this);
                    } catch (MqttException e) {
                        isConnecting = false;
                        reconnect();
                    }
                }
            }, connectDelay, TimeUnit.MILLISECONDS);
        }
    }

    public void disconnect() throws MqttException {
        pahoClient.disconnect();
    }

    public void subscribe(final String topic, final IMessageCallback callback) throws MqttException {
        subscriptionHandler.subscribe(topic, callback);
    }

    public void unsubscribe(final String topic, final IMessageCallback callback) throws MqttException {
        subscriptionHandler.unsubscribe(topic, callback);
    }

    public void publish(final String topic, final Object payload, final int qos, final boolean retained, IMqttActionListener callback) throws MqttException, JsonProcessingException {
        publish(topic, objectMapper.writeValueAsBytes(payload), qos, retained, callback);
    }

    public void publish(final String topic, final byte[] payload, final int qos, final boolean retained, IMqttActionListener callback) throws MqttException {
        pahoClient.publish(topic, payload, qos, retained, null, callback);
    }

    public boolean isConnected() {
        return pahoClient.isConnected();
    }

    @Override
    public void onSuccess(IMqttToken iMqttToken) {
        LOG.info("Connection to broker established");
        connectDelay = 0;
        isConnecting = false;

        try {
            subscriptionHandler.syncSubscriptions();
        } catch (MqttException e) {
            LOG.severe("Error: " + e.getLocalizedMessage());
        }

        if (!hasConnected) {
            connectionCallback.onConnect();
            hasConnected = true;
        } else {
            connectionCallback.onReconnect();
        }
    }

    @Override
    public void onFailure(IMqttToken iMqttToken, Throwable cause) {
        LOG.severe("Connecting to broker failed");
        isConnecting = false;
        connectionCallback.onDisconnect(cause);
        reconnect();
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.severe("Connection lost");
        isConnecting = false;
        connectionCallback.onDisconnect(cause);
        reconnect();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        List<IMessageCallback> callbacks = subscriptionHandler.getCallbacks(topic);
        for (IMessageCallback callback : callbacks) {
            try {
                callback.onRawMessage(topic, message);
            } catch (Exception e) {
                LOG.severe("Error: " + e.getLocalizedMessage());
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    private void reconnect() {
        if (connectDelay == 0) {
            connectDelay = 1000;
        } else {
            connectDelay = Math.min(MAX_RECONNECT_DELAY, connectDelay << 1);
        }

        scheduleConnect(connectDelay);
    }
}
