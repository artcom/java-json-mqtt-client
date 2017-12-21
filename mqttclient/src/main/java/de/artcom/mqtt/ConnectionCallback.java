package de.artcom.mqtt;

public interface ConnectionCallback {
    void onDisconnect(Throwable cause);

    void onConnect();

    void onReconnect();
}