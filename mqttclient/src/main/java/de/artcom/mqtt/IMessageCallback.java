package de.artcom.mqtt;

import org.eclipse.paho.client.mqttv3.MqttMessage;

public interface IMessageCallback {
    void handleMessage(String topic, MqttMessage message);
}