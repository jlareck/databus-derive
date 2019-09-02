package org.dbpedia.databus.derive.io.rdf;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class TripleWrapper extends Triple {

    private long row = -1;

    TripleWrapper(Node s, Node p, Node o) {
        super(s, p, o);
    }

    public long getRow() {
        return row;
    }

    public void setRow(long row) {
        this.row = row;
    }

}
