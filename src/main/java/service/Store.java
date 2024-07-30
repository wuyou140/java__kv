
package service;

import java.io.Closeable;

public interface Store extends Closeable {
    void set(String key, String value);

    String get(String key);

    void rm(String key);
}
