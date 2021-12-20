/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/asl/
 *  
 * or in the "license" file accompanying this file. 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.processing.processors;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory;

import javax.xml.bind.DatatypeConverter;

/**
 * Convert a CSV record into JSON record.
 * 
 * customFieldNames is required. 
 * Optional delimiter other than comma can be configured.
 * Optional jsonFormat can be used for pretty printed json.
 * 
 * Configuration looks like:
 * {
 *     "optionName": "CSVTOJSON",
 *     "customFieldNames": [ "field1", "field2", ... ],
 *     "delimiter": "\\t"
 * }
 * 
 * @author chaocheq
 *
 */
public class CSVToJSONDataConverter implements IDataConverter {
    
    private static String FIELDS_KEY = "customFieldNames";
    private static String DELIMITER_KEY = "delimiter";
    private static String IGNORED_FIELDS_KEY = "ignoredFieldNames";
    private static String ENCODED_FIELDS_KEY = "encodedFieldNames";
    private final List<String> fieldNames;
    private final String delimiter;
    private final IJSONPrinter jsonProducer;
    private final List<String> ignoredFieldNames;
    private final List<String> encodedFieldNames;

    public CSVToJSONDataConverter(Configuration config) {
        fieldNames = config.readList(FIELDS_KEY, String.class);
        delimiter = config.readString(DELIMITER_KEY, ",");
        jsonProducer = ProcessingUtilsFactory.getPrinter(config);
        ignoredFieldNames = config.readList(IGNORED_FIELDS_KEY, String.class, Collections.<String>emptyList());
        encodedFieldNames = config.readList(ENCODED_FIELDS_KEY, String.class, Collections.<String>emptyList());
    }

    @Override
    public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);
        
        // Preserve the NEW_LINE at the end of the JSON record
        if (dataStr.endsWith(NEW_LINE)) {
            dataStr = dataStr.substring(0, (dataStr.length() - NEW_LINE.length()));
        }
        
        String[] columns = dataStr.split(delimiter);
        
        for (int i = 0; i < fieldNames.size(); i++) {
            try {
                recordMap.put(fieldNames.get(i), columns[i]);
            } catch (ArrayIndexOutOfBoundsException e) {
            	LoggerFactory.getLogger(getClass()).debug("Null field in CSV detected");
                recordMap.put(fieldNames.get(i), null);
            } catch (Exception e) {
                throw new DataConversionException("Unable to create the column map", e);
            }
        }

        for (String fieldName: ignoredFieldNames) {
            recordMap.remove(fieldName);
        }

        try {
            for (String fieldName : encodedFieldNames) {
                String value = recordMap.get(fieldName).toString();
                String hash = toMD5(value);
                recordMap.put(fieldName, hash);
            }
        } catch (Exception e) {
            throw new DataConversionException("Unable to parse records.", e);
        }

        String dataJson = jsonProducer.writeAsString(recordMap) + NEW_LINE;
        
        return ByteBuffer.wrap(dataJson.getBytes(StandardCharsets.UTF_8));
    }

    private String toMD5(String data) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
        return DatatypeConverter.printHexBinary(hash);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + 
               "{ delimiter: [" + delimiter + "], " +
               "fields: " + fieldNames.toString() + "}";
    }
}
