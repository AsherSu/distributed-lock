package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockResult;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Set;

public interface LockProcessor {

    Set<LockType> getSupportedTypes();

    LockResult process(LockCommand command);

    void saveSnapshot(ObjectOutputStream out) throws Exception;

    void loadSnapshot(ObjectInputStream in) throws Exception;
}
