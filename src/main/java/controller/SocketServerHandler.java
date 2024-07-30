
package controller;

import dto.ActionDTO;
import dto.ActionTypeEnum;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import service.NormalStore;
import service.Store;
import utils.LoggerUtil;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SocketServerHandler implements Runnable {
    private final Logger LOGGER = LoggerFactory.getLogger(SocketServerHandler.class);
    private Socket socket;
    private Store store;
    private final Map<ActionTypeEnum, ActionStrategy> actions = new HashMap<>();


    public SocketServerHandler(Socket socket, Store store) {
        this.socket = socket;
        this.store = store;
        actions.put(ActionTypeEnum.GET, new GetAction());//初始化映射
        actions.put(ActionTypeEnum.SET, new SetAction());
        actions.put(ActionTypeEnum.RM, new RmAction());
    }

    @Override
    public void run() {
        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());  //获取和发送对象流
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

            // 接收序列化对象
            ActionDTO dto = (ActionDTO) ois.readObject();  //转换为ActionDTO类对象
            LoggerUtil.debug(LOGGER, "[SocketServerHandler][ActionDTO]: {}", dto.toString());  //记录数据信息
            System.out.println("" + dto.toString());

            ActionStrategy action = actions.get(dto.getType());
            if (action != null) {
                action.performAction(dto, store, LOGGER, oos);
            } else {
                // 其他类型
                LoggerUtil.error(LOGGER, null, "Unsupported action type: {}", dto.getType());
            }
            //处理命令逻辑(TODO://改成可动态适配的模式)
            if (dto.getType() == ActionTypeEnum.GET) {
                String value = this.store.get(dto.getKey());
                LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "get action resp" + dto.toString());
                RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, value);
                oos.writeObject(resp);
                oos.flush();
            }
            if (dto.getType() == ActionTypeEnum.SET) {
                this.store.set(dto.getKey(), dto.getValue());
                LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "set action resp" + dto.toString());
                RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, null);
                oos.writeObject(resp);
                oos.flush();
            }
            if (dto.getType() == ActionTypeEnum.RM) {
                this.store.rm(dto.getKey());
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
