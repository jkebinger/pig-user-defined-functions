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
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * An evaluation function to convert a string representing a date into
 * into a long integer seconds since the epoch
 * 
 * Date Format and time zone are currently not customizable - this is a TODO
 *
 */
public class DATE_TO_EPOCH_SECONDS extends EvalFunc<Long> {

	static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
	
	@Override
	public Long exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
	            return null;
        try{
            String dateStr = (String)input.get(0);
            return DATE_FORMAT.parse(dateStr).getTime()/1000;
            
        }catch(ParseException e){
        	return null;
        }
        catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }

	}
	
	public Schema outputSchema(Schema input){
		try{
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("seconds",DataType.LONG));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),tupleSchema, DataType.LONG));
        }catch (Exception e){
                return null;
        }

	}

}
