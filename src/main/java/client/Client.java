
package client;

public interface Client {
    void set(String key, String value);

    String get(String key);

    void rm(String key);
}
