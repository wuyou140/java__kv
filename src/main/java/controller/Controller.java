
package controller;

public interface Controller {
    void set(String key, String value);

    String get(String key);

    void rm(String key);

    void startServer();
}
