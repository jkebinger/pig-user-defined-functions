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

package com.kebinger.pig.evaluation;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * An evaluation function to convert an long integer number of seconds since the epoch
 * into a date
 * Date format and time zone are currently not customizable - this is a TODO
 *
 */
public class EPOCH_SECONDS_TO_DATE extends EvalFunc<String> {

    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
	    "yyyy-MM-dd kk:mm:ss");

    @Override
    public String exec(Tuple input) throws IOException {
	if (input == null || input.size() == 0)
	    return null;
	try {
	    Long seconds = (Long) input.get(0);
	    Date date = new Date(seconds * 1000L);
	    return DATE_FORMAT.format(date);
	} catch (Exception e) {
	    throw WrappedIOException.wrap(
		    "Caught exception processing input row ", e);
	}

    }

    public Schema outputSchema(Schema input) {
	try {
	    Schema tupleSchema = new Schema();
	    tupleSchema.add(new Schema.FieldSchema("date", DataType.CHARARRAY));
	    return new Schema(new Schema.FieldSchema(getSchemaName(this
		    .getClass().getName().toLowerCase(), input), tupleSchema,
		    DataType.LONG));
	} catch (Exception e) {
	    return null;
	}

    }

}
