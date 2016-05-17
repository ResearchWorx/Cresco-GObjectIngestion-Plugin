package com.researchworx.cresco.plugins.skeleton;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.library.plugin.core.CPlugin;

@AutoService(CPlugin.class)
public class Plugin extends CPlugin {
    public void setExecutor() {
        setExec(new Executor(this));
    }

    public void start() {
        /*
         *  Insert your startup code here
         */
    }

    @Override
    public void cleanUp() {
        /*
         *  Insert your shutdown / clean up code here
         */
    }
}
