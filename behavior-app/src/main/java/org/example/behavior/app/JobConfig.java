package org.example.behavior.app;

import org.example.behavior.common.exception.BehaviorException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Loads {@code application.properties} from the classpath and lets command line
 * {@code --key=value} arguments override any entry, so one fat-jar serves every
 * environment.
 */
public final class JobConfig {

    private final Properties properties;

    private JobConfig(Properties properties) {
        this.properties = properties;
    }

    public static JobConfig load(String[] args) {
        Properties properties = new Properties();
        try (InputStream in = JobConfig.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in != null) {
                properties.load(in);
            }
        } catch (IOException e) {
            throw new BehaviorException("Failed to load application.properties", e);
        }

        if (args != null) {
            for (String arg : args) {
                if (arg == null || !arg.startsWith("--")) {
                    continue;
                }
                String body = arg.substring(2);
                int separator = body.indexOf('=');
                if (separator > 0) {
                    properties.setProperty(
                            body.substring(0, separator), body.substring(separator + 1));
                }
            }
        }
        return new JobConfig(properties);
    }

    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : Integer.parseInt(value.trim());
    }

    public long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : Long.parseLong(value.trim());
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : Boolean.parseBoolean(value.trim());
    }
}
