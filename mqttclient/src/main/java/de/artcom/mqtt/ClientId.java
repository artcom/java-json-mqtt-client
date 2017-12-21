package de.artcom.mqtt;

import java.util.UUID;

public class ClientId {
    public static String generate(String applicationId, String deviceId) {
        return applicationId + "-" + deviceId + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
