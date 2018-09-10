package com.scylladb.tools;

import java.util.ArrayList;
import java.util.List;

public class Metrics {
    public long statementsSent;
    public long statementsFailed;
    public long statementsProcessed;
    public long batchesProcessed;
    public long keysProcessed;
    public long preparationsDone;
    public long preparationsFailed;
    public long typesCreated;

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
    }

    public Metrics newChild() {
        Metrics m = new Metrics();
        if (children == null) {
            children = new ArrayList<>();
        }
        children.add(m);
        return m;
    }

    public Metrics sum() {
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