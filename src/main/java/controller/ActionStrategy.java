package controller;

import dto.ActionDTO;
import org.slf4j.Logger;
import service.Store;

import java.io.IOException;
import java.io.ObjectOutputStream;

public interface ActionStrategy {
    void performAction(ActionDTO dto, Store store, Logger LOGGER,ObjectOutputStream oos) throws IOException;
}
