package de.artcom.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.util.*;

class SubscriptionHandler {
    private final IMqttAsyncClient client;
    private final Map<String, List<IMessageCallback>> subscriptions = new HashMap<>();

    private final Set<String> topicsToUnsubscribe = new HashSet<>();

    SubscriptionHandler(IMqttAsyncClient client) {
        this.client = client;
    }

    void subscribe(final String topic, final IMessageCallback callback) throws MqttException {
        synchronized (subscriptions) {
            List<IMessageCallback> callbacks = subscriptions.get(topic);
            if (callbacks == null) {
                callbacks = new ArrayList<>();
                subscriptions.put(topic, callbacks);

                if (client.isConnected()) {
                    client.subscribe(topic, 2);
                }
            }

            callbacks.add(callback);
        }
    }

    void unsubscribe(final String topic, final IMessageCallback callback) throws MqttException {
        synchronized (subscriptions) {
            List<IMessageCallback> callbacks = subscriptions.get(topic);
            if (callbacks != null) {
                callbacks.remove(callback);

                if (callbacks.isEmpty()) {
                    if (client.isConnected()) {
                        client.unsubscribe(topic);
                    } else {
                        topicsToUnsubscribe.add(topic);
                    }

                    subscriptions.remove(topic);
                }
            }
        }
    }

    void syncSubscriptions() throws MqttException {
        synchronized (subscriptions) {
            for (String topic : subscriptions.keySet()) {
                client.subscribe(topic, 2);
            }

            for (String topic : topicsToUnsubscribe) {
                client.unsubscribe(topic);
            }

            topicsToUnsubscribe.clear();
        }
    }

    List<IMessageCallback> getCallbacks(String topic) {
        synchronized (subscriptions) {
            List<IMessageCallback> result = new ArrayList<>();
            for (Map.Entry<String, List<IMessageCallback>> entry : subscriptions.entrySet()) {
                if (matches(topic, entry.getKey())) {
                    result.addAll(entry.getValue());
                }
            }

            return result;
        }
    }

    static boolean matches(String topic, String subscription) {
        List<String> topLevels = splitLevels(topic);
        List<String> subLevels = splitLevels(subscription);

        if (subLevels.indexOf("#") == -1) {
            if (topLevels.size() != subLevels.size()) {
                return false;
            }
        } else {
            if (topLevels.size() < subLevels.size() - 1) {
                return false;
            }
        }

        Iterator<String> topIter = topLevels.iterator();
        for (String subLevel : subLevels) {
            if ("#".equals(subLevel)) {
                return true;
            }

            if (!subLevel.equals(topIter.next()) && !"+".equals(subLevel)) {
                return false;
            }
        }

        return true;
    }

    private static List<String> splitLevels(String topic) {
        return "/".equals(topic)
                ? Arrays.asList("", "")
                : Arrays.asList(topic.split("/"));
    }
}
