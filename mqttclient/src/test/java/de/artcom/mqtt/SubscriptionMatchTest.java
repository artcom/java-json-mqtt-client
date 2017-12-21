package de.artcom.mqtt;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.Assert.assertFalse;

public class SubscriptionMatchTest {
    private static final Logger LOG = Logger.getLogger(SubscriptionMatchTest.class.getSimpleName());

    private static final Map<String, String[]> matchingTopics = new HashMap<>();
    private static final Map<String, String[]> mismatchingTopics = new HashMap<>();

    @BeforeClass
    public static void setup() {
        matchingTopics.put("/", new String[]{"/"});
        matchingTopics.put("foo", new String[]{"foo"});
        matchingTopics.put("/foo", new String[]{"/foo"});
        matchingTopics.put("+", new String[]{"foo", "bar"});
        matchingTopics.put("/+", new String[]{"/", "/foo", "/bar"});
        matchingTopics.put("/#", new String[]{"/", "/foo", "/foo/bar"});
        matchingTopics.put("#", new String[]{"/", "foo", "foo/bar", "/foo/bar"});
        matchingTopics.put("foo/+", new String[]{"foo/bar"});
        matchingTopics.put("foo/+/baz", new String[]{"foo/bar/baz", "foo//baz"});
        matchingTopics.put("foo/+/+/bla", new String[]{"foo/bar/baz/bla"});
        matchingTopics.put("foo/#", new String[]{"foo/bar", "foo/bar/baz", "foo/bar/baz/bla"});
        matchingTopics.put("foo/+/#", new String[]{"foo/bar", "foo/bar/baz", "foo/bar/baz/bla"});
        matchingTopics.put("foo/+/baz/#", new String[]{"foo/bar/baz", "foo/bar/baz/bla"});

        mismatchingTopics.put("foo", new String[]{"bar"});
        mismatchingTopics.put("/foo", new String[]{"foo", "/bar"});
        mismatchingTopics.put("+", new String[]{"foo/bar", "/foo"});
        mismatchingTopics.put("/+", new String[]{"foo", "/foo/bar"});
        mismatchingTopics.put("foo/+", new String[]{"foo/bar/baz"});
        mismatchingTopics.put("foo/+/baz", new String[]{"foo", "foo/bar", "foo/bar/bla", "foo/bar/baz/bla"});
        mismatchingTopics.put("foo/+/+/bla", new String[]{"foo/bar/baz", "foo/bar/bla", "foo/bar/baz/bla/blo"});
        mismatchingTopics.put("foo/#", new String[]{"/"});
        mismatchingTopics.put("foo/+/#", new String[]{"/", "foo"});
        mismatchingTopics.put("foo/+/baz/#", new String[]{"foo/bar/bla/baz"});
        mismatchingTopics.put("foo2.0", new String[]{"foo200"});
    }

    @Test
    public void matchesTopics() {
        boolean failed = false;
        for (Map.Entry<String, String[]> match : matchingTopics.entrySet()) {
            for (String topic : match.getValue()) {
                if (!SubscriptionHandler.matches(topic, match.getKey())) {
                    failed = true;
                    LOG.severe("Topic '" + topic + "' does not match topic filter '" + match.getKey() + "'");
                }
            }
        }
        assertFalse(failed);
    }

    @Test
    public void mismatchesTopics() {
        boolean failed = false;
        for (Map.Entry<String, String[]> mismatch : mismatchingTopics.entrySet()) {
            for (String topic : mismatch.getValue()) {
                if (SubscriptionHandler.matches(topic, mismatch.getKey())) {
                    failed = true;
                    LOG.severe("Topic '" + topic + "' matches topic filter '" + mismatch.getKey() + "'");
                }
            }
        }
        assertFalse(failed);
    }
}
