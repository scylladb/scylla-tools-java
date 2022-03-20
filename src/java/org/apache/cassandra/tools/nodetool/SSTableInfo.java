package org.apache.cassandra.tools.nodetool;

import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import io.airlift.command.Arguments;
import io.airlift.command.Command;

@Command(name = "sstableinfo", description = "Information about sstables per keyspace/table")
public class SSTableInfo extends NodeToolCmd {
    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    private final static String BETWEEN_LOWER_AND_UPPER = "(?<=\\p{Ll})(?=\\p{Lu})";
    private final static String BEFORE_UPPER_AND_LOWER = "(?<=\\p{L})(?=\\p{Lu}\\p{Ll})";

    private final static Pattern SPLIT_CAMEL_CASE = Pattern
            .compile(BETWEEN_LOWER_AND_UPPER + "|" + BEFORE_UPPER_AND_LOWER);

    public static String splitCamelCase(String s) {
        return SPLIT_CAMEL_CASE.splitAsStream(s).map((v) -> v.toLowerCase()).collect(joining(" "));
    }

    @Override
    protected void execute(NodeProbe probe) {
        String keyspace = null;
        String table = null;

        if (!args.isEmpty()) {
            keyspace = args.remove(0);
        }

        PrintStream out = System.out;

        out.println();

        ArrayList<String> fields = null;
        HashMap<String, String> printNames = new HashMap<>();
        CompositeType type = null;
        int maxLen = 0;
        int baseLen = 8;
        int indent = 4;

        do {
            if (!args.isEmpty()) {
                table = args.remove(0);
            }

            String kstable[] = new String[] { "keyspace", "table" };
            try {
                List<?> result = (List<?>) probe.getSSTableInfo(keyspace, table);

                for (Object o : result) {
                    CompositeData data = (CompositeData) o;

                    Object[] sstables = (Object[]) data.get("SSTables");

                    if (sstables == null) {
                        continue;
                    }

                    for (String k : kstable) {
                        out.format("%1$" + baseLen + "s : %2$s%n", k, data.get(k));
                    }

                    out.format("%1$" + baseLen + "s :%n", "sstables");

                    int i = 0;
                    for (Object so : sstables) {
                        out.format("%" + baseLen + "s :%n", i++);

                        CompositeData sst = (CompositeData) so;

                        if (fields == null) {
                            final CompositeType t = sst.getCompositeType();
                            type = t;
                            fields = new ArrayList<>(type.keySet());
                            Collections.sort(fields, (s1, s2) -> {
                                OpenType<?> t1 = t.getType(s1);
                                OpenType<?> t2 = t.getType(s2);
                                return t1.getClassName().compareTo(t2.getClassName());
                            });

                            for (String s : fields) {
                                String name = splitCamelCase(s);
                                maxLen = Math.max(maxLen, name.length());
                                printNames.put(s, name);
                            }
                        }

                        for (String f : fields) {
                            Object val = sst.get(f);
                            String name = printNames.get(f);
                            printValue(out, name, val, indent, maxLen);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error getting sstable info: " + e);
                return;
            }
        } while (!args.isEmpty());
    }

    private void printTabular(PrintStream out, TabularData table, int indent) {
        List<String> index = table.getTabularType().getIndexNames();
        String kk = index.get(0);
        String vk = null;
        for (String s : table.getTabularType().getRowType().keySet()) {
            if (!kk.equals(s)) {
                vk = s;
                break;
            }
        }
        int maxLen = 0;
        for (Object o : table.values()) {
            CompositeData data = (CompositeData) o;
            String name = String.valueOf(data.get(kk));
            maxLen = Math.max(maxLen, name.length());
        }
        for (Object o : table.values()) {
            CompositeData data = (CompositeData) o;
            String name = String.valueOf(data.get(kk));
            Object value = data.get(vk);
            printValue(out, name, value, indent, maxLen);
        }
    }
    
    private void printValue(PrintStream out, String name, Object val, int indent, int maxLen) {
        if (val == null) {
            return;
        }
        out.format("%" + (maxLen + indent) + "s : ", name);

        if (val instanceof TabularData) {
            out.println();
            printTabular(out, (TabularData) val, maxLen/2 + indent);
        } else {
            out.println(val);
        }
    }
}
