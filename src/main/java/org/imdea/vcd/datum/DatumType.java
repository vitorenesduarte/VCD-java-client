/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.imdea.vcd.datum;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 *
 * @author user
 */
public enum DatumType {
    MESSAGE_SET(MessageSet.getClassSchema()),
    STATUS(Status.getClassSchema());
    
    private final SpecificDatumReader<Object> reader;
    private final SpecificDatumWriter<Object> writer;
    
    private DatumType(Schema schema){
        this.reader = new SpecificDatumReader<>(schema);
        this.writer = new SpecificDatumWriter<>(schema);
    }

    public SpecificDatumReader<Object> getReader() {
        return reader;
    }

    public SpecificDatumWriter<Object> getWriter() {
        return writer;
    }
}
