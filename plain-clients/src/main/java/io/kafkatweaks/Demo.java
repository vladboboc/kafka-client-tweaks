package io.kafkatweaks;

import io.kafkatweaks.common.Args;

/** One runnable chapter demo. Implementations live next to the chapter they belong to. */
@FunctionalInterface
public interface Demo {

    void run(Args args) throws Exception;
}
