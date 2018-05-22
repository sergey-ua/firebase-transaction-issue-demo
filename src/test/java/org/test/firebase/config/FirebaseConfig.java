package org.test.firebase.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class FirebaseConfig {
    private static FirebaseConfig ourInstance = new FirebaseConfig();

    public static FirebaseConfig getInstance() {
        return ourInstance;
    }

    private FirebaseConfig() {
    }

    public String getDatabaseURL() {
        return System.getProperty("databaseUrl");
    }

    public InputStream getCertAsInputStream() {
        try {
            return new FileInputStream(System.getProperty("cert"));
        } catch (FileNotFoundException e) {
            throw  new IllegalStateException("Problem reading cert account", e);
        }
    }
}
