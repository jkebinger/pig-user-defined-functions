/*
 * Copyright 2009 James Kebinger
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.kebinger.pig.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A load function based on PigStorage that implements part of the CSV "standard"
 * This loader properly supports double-quoted fields that contain commas and other
 * double-quotes escaped with backslashes.
 * 
 * The following fields are all parsed as one tuple, per each line
 * "the man, he said ""hello"""
 * "one,two,three"
 * 
 * This version supports pig 0.3
 *
 */
public class CSVLoader extends Utf8StorageConverter implements LoadFunc {
    protected BufferedPositionedInputStream in = null;
    protected final Log mLog = LogFactory.getLog(getClass());
    private static final byte DOUBLE_QUOTE = '"';
    private static final byte FIELD_DEL = ',';
    private static final byte RECORD_DEL = '\n';
    
    long end = Long.MAX_VALUE;
    

    private ByteArrayOutputStream mBuf = null;
    private ArrayList<Object> mProtoTuple = null;
    private int os;
    private static final int OS_UNIX = 0;
    private static final int OS_WINDOWS = 1;


    public CSVLoader() {
	os = OS_UNIX;
	if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
	    os = OS_WINDOWS;
	}
    }

    @Override
    public Schema determineSchema(String arg0, ExecType arg1, DataStorage arg2)
	    throws IOException {
	// Do nothing
	return null;
    }

    @Override
    public void fieldsToRead(Schema schema) {
	// Do nothing

    }

    @Override
    public Tuple getNext() throws IOException {
	if (in == null || in.getPosition() > end) {
	    return null;
	}
	boolean inQuotedField = false;
	boolean escapedQuoteEmitted = false;
	int lastChar = -1;

	if (mBuf == null) {
	    mBuf = new ByteArrayOutputStream(4096);
	}
	mBuf.reset();
	//TODO: redo this in a more elegant way
	while (true) {

	    int b = in.read();
	    if (inQuotedField)
	    {
		if (b == DOUBLE_QUOTE && lastChar == DOUBLE_QUOTE && !escapedQuoteEmitted)
		{
		    mBuf.write(DOUBLE_QUOTE);
		    lastChar = -1;
		    escapedQuoteEmitted = true;
		}
		else if (b == DOUBLE_QUOTE && lastChar == DOUBLE_QUOTE && escapedQuoteEmitted)
		{
		    escapedQuoteEmitted = false;
		}
		else
		
		if (lastChar == DOUBLE_QUOTE && (b == FIELD_DEL || b == RECORD_DEL)){
		    
		    inQuotedField = false;
		    readField();
		}
		else if ( b == DOUBLE_QUOTE )
		{
		    // do nothing for now - check next go around
		}
		else{
		    mBuf.write(b);
		}
	    }
	    else if (b == DOUBLE_QUOTE)
	    {
		
		inQuotedField = true;
	    } else if (b == FIELD_DEL) {
		readField(); // end of the field
	    }
	    else if (b == RECORD_DEL) {
		readField();
		Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);
		mProtoTuple = null;
		return t;
	    } else if (b == -1) {
		// hit end of file
		return null;
	    } 
	    else {
		mBuf.write(b);
	    }
	    
	    lastChar = b;
	}
    }

    @Override
    public void bindTo(String fileName, BufferedPositionedInputStream in,
	    long offset, long end) throws IOException {
	this.in = in;
        this.end = end;
        
        // Since we are not block aligned we throw away the first
        // record and count on a different instance to read it
        if (offset != 0) {
            getNext();
        }

    }

    private void readField() {
	if (mProtoTuple == null)
	    mProtoTuple = new ArrayList<Object>();
	if (mBuf.size() == 0) {
	    // NULL value
	    mProtoTuple.add(null);
	} else {
	    // TODO, once this can take schemas, we need to figure out
	    // if the user requested this to be viewed as a certain
	    // type, and if so, then construct it appropriately.
	    byte[] array = mBuf.toByteArray();
	    if (array[array.length - 1] == '\r' && os == OS_WINDOWS) {
		// This is a java 1.6 function. Until pig officially moves to
		// 1.6 we can't use this.
		// array = Arrays.copyOf(array, array.length-1);
		byte[] tmp = new byte[array.length - 1];
		for (int i = 0; i < array.length - 1; i++)
		    tmp[i] = array[i];
		array = tmp;
	    }

	    if (array.length == 0)
		mProtoTuple.add(null);
	    else
		mProtoTuple.add(new DataByteArray(array));
	}
	mBuf.reset();
    }

}
