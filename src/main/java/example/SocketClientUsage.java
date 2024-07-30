
package example;

import client.Client;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 8090;
        Client client = new SocketClient(host, port);
        //向服务器存储数据
        client.set("ceshi1","1");
        client.set("ceshi2","2");
        client.set("ceshi3","3");
        //让服务器删除数据
        client.rm("ceshi1");
        //查询数据
        client.get("ceshi2");
    }
}
