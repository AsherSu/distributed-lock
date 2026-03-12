package cn.ashersu.lock.lock;

import cn.ashersu.lock.command.LockCommand;
import cn.ashersu.lock.statemachine.LockResult;
import cn.ashersu.lock.statemachine.LockType;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public interface LockProcessor {

    LockType getSupportedType();

    LockResult process(LockCommand command);

    void saveSnapshot(ObjectOutputStream out) throws Exception;

    void loadSnapshot(ObjectInputStream in) throws Exception;
}
