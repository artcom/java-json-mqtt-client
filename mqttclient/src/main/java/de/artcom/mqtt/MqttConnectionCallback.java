package de.artcom.mqtt;

public interface MqttConnectionCallback {
    void onDisconnect(Throwable cause);

    void onConnect();

    void onReconnect();
}