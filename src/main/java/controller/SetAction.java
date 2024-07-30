package controller;

import dto.ActionDTO;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import org.slf4j.Logger;
import service.Store;
import utils.LoggerUtil;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class SetAction implements ActionStrategy{
    @Override
    public void performAction(ActionDTO dto, Store store, Logger LOGGER, ObjectOutputStream oos) throws IOException {
        store.set(dto.getKey(), dto.getValue());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "set action resp" + dto.toString());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, null);
        oos.writeObject(resp);
        oos.flush();
    }
}
