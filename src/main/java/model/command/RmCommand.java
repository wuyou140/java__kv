
package model.command;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class RmCommand extends AbstractCommand {
    private String key;

    public RmCommand(String key) {
        super(CommandTypeEnum.RM);
        this.key = key;
    }
}
