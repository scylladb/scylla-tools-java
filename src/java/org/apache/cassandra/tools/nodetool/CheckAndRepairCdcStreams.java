package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import io.airlift.command.Command;

@Command(name = "checkAndRepairCdcStreams",
        description = "Checks that CDC streams reflect current cluster topology and regenerates them if not.\n"
        + "Warning: DO NOT use this while performing other administrative tasks, like bootstrapping or"
        + " decommissioning a node.")
public class CheckAndRepairCdcStreams extends NodeToolCmd {
    @Override
    protected void execute(NodeProbe probe) {
        PrintStream out = System.out;
        out.println();
        probe.checkAndRepairCdcStreams();
    }
}
