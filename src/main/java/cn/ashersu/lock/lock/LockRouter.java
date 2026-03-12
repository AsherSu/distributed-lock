package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockResult;

import java.util.EnumMap;
import java.util.Map;

public class LockRouter {

    private final Map<LockType, LockProcessor> processors = new EnumMap<>(LockType.class);

    public void registerProcessor(LockProcessor processor) {
        processors.put(processor.getSupportedType(), processor);
    }

    public LockResult routeAndProcess(LockCommand command) {
        LockProcessor processor = processors.get(command.getLockType());

        if (processor != null) {
            // 执行业务
            return processor.process(command);
        } else {
            return LockResult.error(command.getLockType(),"Unsupported Lock Type: " + command.getLockType());
        }
    }

    public Iterable<LockProcessor> getAllProcessors() {
        return processors.values();
    }
}
