package de.artcom.mqtt;

import org.eclipse.paho.client.mqttv3.MqttMessage;

interface IMqttMessageCallback {
    void handleMessage(String topic, MqttMessage message);
}