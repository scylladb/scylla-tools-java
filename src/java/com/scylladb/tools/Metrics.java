package com.scylladb.tools;

import java.util.ArrayList;
import java.util.List;

/**
 * Non thread-safe metrics amalgation of sstable loader stats.
 *
 * @author calle
 *
 */
public class Metrics {
    public long statementsSent;
    public long statementsFailed;
    public long statementsProcessed;
    public long batchesProcessed;
    public long keysProcessed;
    public long preparationsDone;
    public long preparationsFailed;
    public long typesCreated;
    public long bytesProcessed;
    /** to deal with "extra size" found during processing */ 
    public long additionalBytes;
    
    private List<Metrics> children;

    public Metrics() {
    }

    public Metrics(Metrics m) {
        this.statementsSent = m.statementsSent;
        this.statementsFailed = m.statementsFailed;
        this.statementsProcessed = m.statementsProcessed;
        this.batchesProcessed = m.batchesProcessed;
        this.keysProcessed = m.keysProcessed;
        this.preparationsDone = m.preparationsDone;
        this.preparationsFailed = m.preparationsFailed;
        this.typesCreated = m.typesCreated;
        this.bytesProcessed = m.bytesProcessed;
        this.additionalBytes = m.additionalBytes;
    }

    public void add(Metrics m) {
        this.statementsSent += m.statementsSent;
        this.statementsFailed += m.statementsFailed;
        this.statementsProcessed += m.statementsProcessed;
        this.batchesProcessed += m.batchesProcessed;
        this.keysProcessed += m.keysProcessed;
        this.preparationsDone += m.preparationsDone;
        this.preparationsFailed += m.preparationsFailed;
        this.typesCreated += m.typesCreated;
        this.bytesProcessed += m.bytesProcessed;
        this.additionalBytes += m.additionalBytes;
    }

    /**
     * Allow the concept of "children" to enable sub-processes (threads) to have
     * their own metrics than can be updated in a singlular path, yet be
     * summarized (potentially inexact) by us
     */
    public synchronized Metrics newChild() {
        Metrics m = new Metrics();
        if (children == null) {
            children = new ArrayList<>();
        }
        children.add(m);
        return m;
    }

    /**
     * Generate a new metrics instance with the (inexact) sum of me and all my
     * children. We only sync the children array really, all values are picked
     * up as perceived by us, potentially slightly askew.
     *
     * @return
     */
    public synchronized Metrics sum() {
        if (children == null) {
            return this;
        }
        Metrics m = new Metrics(this);
        for (Metrics o : children) {
            m.add(o);
        }
        return m;
    }
}