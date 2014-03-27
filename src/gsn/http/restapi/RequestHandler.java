/**
 * Global Sensor Networks (GSN) Source Code
 * Copyright (c) 2006-2014, Ecole Polytechnique Federale de Lausanne (EPFL)
 *
 * This file is part of GSN.
 *
 * GSN is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * GSN is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GSN.  If not, see <http://www.gnu.org/licenses/>.
 *
 * File: src/gsn/http/restapi/RequestHandler.java
 *
 * @author Sofiane Sarni
 * @author Ivo Dimitrov
 * @author Milos Stojanovic
 * @author Jean-Paul Calbimonte
 *
 */

package gsn.http.restapi;

import gsn.Main;
import gsn.Mappings;
import gsn.beans.DataField;
import gsn.beans.VSensorConfig;
import gsn.http.ac.DataSource;
import gsn.http.ac.User;
import gsn.utils.geo.GridTools;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.collections.KeyValue;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

public class RequestHandler {
    private static transient Logger logger = Logger.getLogger(RequestHandler.class);

    public static enum ErrorType {NO_SUCH_SENSOR, NO_SUCH_USER, NO_SENSOR_ACCESS, UNKNOWN_REQUEST, MALFORMED_DATE_FROM_TO, MALFORMED_DATE_DATE_FIELD, ERROR_IN_REQUEST}

    private String format = RestServlet.FORMAT_JSON;
    
    
    
    //request handling
    public RestResponse getAllSensors(User user) {
    	RestResponse restResponse = userExists(user);
        if (restResponse != null) { //error occured
            return restResponse;
        }

        restResponse = new RestResponse();

        String filename = String.format(stringConstantsProperties.getProperty("FILENAME_MULTIPLE_SENSORS"), datetime);
        setRestResponseParams(restResponse, filename);

        Iterator<VSensorConfig> vsIterator = Mappings.getAllVSensorConfigs();
        List<VirtualSensor> listOfSensors = new ArrayList<VirtualSensor>(); 
        while (vsIterator.hasNext()) {

            VSensorConfig sensorConfig = vsIterator.next();
            VirtualSensor sensor = new VirtualSensor();

            String vs_name = sensorConfig.getName();
            if (userHasAccessToVirtualSensor(user, vs_name) != null){
            	//user doesn't have access to this sensor
            	continue;
            }
            
            sensor.setMetadata(createHeaderMap(sensorConfig));
            sensor.appendFields(sensorConfig.getOutputStructure());
            
            listOfSensors.add(sensor);
        }

        restResponse.setResponse(VirtualSensor.generateFileContent(listOfSensors, format));

        return restResponse;
    }
    
    public RestResponse getMeasurementsForSensor(User user, String sensor, String from, String to, String size) {
        RestResponse restResponse = userHasAccessToVirtualSensor(user, sensor);
        if (restResponse != null) { //error occured
            return restResponse;
        }

        restResponse = new RestResponse();

        String filename = String.format(stringConstantsProperties.getProperty("FILENAME_SENSOR_FIELDS"), sensor, datetime);
        setRestResponseParams(restResponse, filename);

        long fromAsLong = 0;
        long toAsLong = 0;
        try {
            fromAsLong = new java.text.SimpleDateFormat(stringConstantsProperties.getProperty("ISO_FORMAT")).parse(from).getTime();
            toAsLong = new java.text.SimpleDateFormat(stringConstantsProperties.getProperty("ISO_FORMAT")).parse(to).getTime();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            restResponse = errorResponse(ErrorType.MALFORMED_DATE_FROM_TO, user, sensor);
            return restResponse;
        }
        
        VSensorConfig sensorConfig = Mappings.getConfig(sensor);
        VirtualSensor sensorObj = new VirtualSensor();
        
        sensorObj.setMetadata(createHeaderMap(sensorConfig));
        sensorObj.appendField(new DataField("time", "Time"));
        sensorObj.appendField(new DataField("timestamp", "Time"));

        Vector<Double> stream = new Vector<Double>();
        Vector<Long> timestamps = new Vector<Long>();
        ArrayList<Vector<Double>> elements  = new ArrayList<Vector<Double>>();
        ArrayList<String> fields = new ArrayList<String>();


        for (DataField df : sensorConfig.getOutputStructure()) {
            fields.add(df.getName().toLowerCase());
            sensorObj.appendField(df);
        }

        Connection conn = null;
        ResultSet resultSet = null;
        boolean restrict = false;

        if (size != null)  {
            restrict = true;
        }

        try {
            conn = Main.getStorage(sensor).getConnection();

            StringBuilder query;
            if (restrict) {
                Integer window = new Integer(size);
                query = new StringBuilder("select * from ")
                        .append(sensor)
                        .append(" where timed >= ")
                        .append(fromAsLong)
                        .append(" and timed <=")
                        .append(toAsLong)
                        .append(" order by timed desc")
                        .append(" limit 0,"+(window+1));
            } else {
                query = new StringBuilder("select * from ")
                        .append(sensor)
                        .append(" where timed >= ")
                        .append(fromAsLong)
                        .append(" and timed <=")
                        .append(toAsLong);
            }
            
            resultSet = Main.getStorage(sensor).executeQueryWithResultSet(query, conn);
            
            if (restrict) {
            	resultSet.afterLast();
            	while (resultSet.previous()) {
                    timestamps.add(resultSet.getLong("timed"));
                    for (String fieldname : fields) {
                        stream.add(getDouble(resultSet, fieldname));
                    }
                    elements.add(stream);
                    stream = new Vector<Double>();
                }
            } else {
            	while (resultSet.next()) {
            		timestamps.add(resultSet.getLong("timed"));
            		for (String fieldname : fields) {
                        stream.add(getDouble(resultSet, fieldname));
                    }
                    elements.add(stream);
                    stream = new Vector<Double>();
                }
            }
            sensorObj.setValues(elements, timestamps);

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            restResponse = errorResponse(ErrorType.ERROR_IN_REQUEST, user, sensor);
            return restResponse;
        } finally {
            Main.getStorage(sensor).close(resultSet);
            Main.getStorage(sensor).close(conn);
        }

        List<VirtualSensor> listSens = new LinkedList<VirtualSensor>();
        listSens.add(sensorObj);

        restResponse.setResponse(VirtualSensor.generateFileContent(listSens, format));

        return restResponse;
    }

    public RestResponse getMeasurementsForSensorField(User user, String sensor, String field, String from, String to, String size) {
        RestResponse restResponse = userHasAccessToVirtualSensor(user, sensor);
        if (restResponse != null) { //error occured
            return restResponse;
        }

        restResponse = new RestResponse();
        
        String filename = String.format(stringConstantsProperties.getProperty("FILENAME_SENSOR_FIELD"), sensor, field, datetime);
        setRestResponseParams(restResponse, filename);

        boolean errorFlag = false;
        long fromAsLong = 0;
        long toAsLong = 0;
        try {
            fromAsLong = new java.text.SimpleDateFormat(stringConstantsProperties.getProperty("ISO_FORMAT")).parse(from).getTime();
            toAsLong = new java.text.SimpleDateFormat(stringConstantsProperties.getProperty("ISO_FORMAT")).parse(to).getTime();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            restResponse = errorResponse(ErrorType.MALFORMED_DATE_FROM_TO, user, sensor);
            return restResponse;
        }
        
        VSensorConfig sensorConfig = Mappings.getConfig(sensor);
        VirtualSensor sensorObj = new VirtualSensor();
        
        sensorObj.setMetadata(createHeaderMap(sensorConfig));
        sensorObj.appendField(new DataField("time", "Time"));
        sensorObj.appendField(new DataField("timestamp", "Time"));
        for (DataField df: sensorConfig.getOutputStructure()){
        	if (df.getName().equals(field)){
        		 sensorObj.appendField(df);
        		 break;
        	}
        }
        
        Vector<Double> stream = new Vector<Double>();
        ArrayList<Vector<Double>> elements  = new ArrayList<Vector<Double>>();
        Vector<Long> timestamps = new Vector<Long>();


        if (size != null)  {
            Integer window = new Integer(size);
            Connection conn = null;
            ResultSet resultSet = null;

            try {
                conn = Main.getStorage(sensor).getConnection();
                StringBuilder query = new StringBuilder("select timed, ")
                        .append(field)
                        .append(" from ")
                        .append(sensor)
                        .append(" where timed >= ")
                        .append(fromAsLong)
                        .append(" and timed <=")
                        .append(toAsLong)
                        .append(" order by timed desc")
                        .append(" limit 0,"+(window+1));//why size+1?

                resultSet = Main.getStorage(sensor).executeQueryWithResultSet(query, conn);

                resultSet.afterLast();
            	while (resultSet.previous()) {
                    timestamps.add(resultSet.getLong(1));
                    stream.add(getDouble(resultSet,field));
                    elements.add(stream);
                    stream = new Vector<Double>();
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                errorFlag = true;
            } finally {
                Main.getStorage(sensor).close(resultSet);
                Main.getStorage(sensor).close(conn);
            }
        } else {
            errorFlag = !getData(sensor, field, fromAsLong, toAsLong, elements, timestamps);
        }
        
        sensorObj.setValues(elements, timestamps);

        if (errorFlag) {
            restResponse = errorResponse(ErrorType.ERROR_IN_REQUEST, user, sensor);
            return restResponse;
        }

        List<VirtualSensor> listSens = new LinkedList<VirtualSensor>();
        listSens.add(sensorObj);

        restResponse.setResponse(VirtualSensor.generateFileContent(listSens, format));

        return restResponse;
    }

    //TODO implementation for csv
    public RestResponse getGridData(User user, String sensor, String date) {
        RestResponse restResponse = userHasAccessToVirtualSensor(user, sensor);
        if (restResponse != null) { //error occured
            return restResponse;
        }

        restResponse = new RestResponse();

        long timestamp = -1;
        try {
            timestamp = new java.text.SimpleDateFormat(stringConstantsProperties.getProperty("ISO_FORMAT")).parse(date).getTime();
        } catch (ParseException e) {
            logger.warn("Timestamp is badly formatted: " + date);
        }
        if (timestamp == -1) {
            return errorResponse(ErrorType.MALFORMED_DATE_DATE_FIELD, user, sensor);
        }

        try {
        	restResponse.setResponse(GridTools.executeQueryForGridAsJSON(sensor, timestamp));
        } catch (OutOfMemoryError e){
        	JSONObject jsonObject = new JSONObject();
        	jsonObject.put("error", stringConstantsProperties.getProperty("ERROR_OUT_OF_MEMORY_ERROR"));
        	restResponse.setResponse(jsonObject.toJSONString());
            restResponse.setHttpStatus(RestResponse.HTTP_STATUS_ERROR);
            restResponse.setType(RestResponse.JSON_CONTENT_TYPE);
            return restResponse;
        }
        
        restResponse.setHttpStatus(RestResponse.HTTP_STATUS_OK);
        restResponse.setType(RestResponse.JSON_CONTENT_TYPE);

        logger.warn(restResponse.toString());
        return restResponse;
    }

    //TODO Access Control missing
    public RestResponse getPreviewMeasurementsForSensorField(User user, String sensor, String field, String from, String to, String size) {
        /*
    	RestResponse restResponse = userHasAccessToVirtualSensor(user, sensor);
        if (restResponse != null) { //error occured
            return restResponse;
        }
    	*/
    	RestResponse restResponse = new RestResponse();
    	
    	String filename = String.format(stringConstantsProperties.getProperty("FILENAME_PREVIEW_SENSOR_FIELD"), sensor, field, datetime);
        setRestResponseParams(restResponse, filename);

        List<Vector<Double>> elements = new ArrayList<Vector<Double>>();
        Vector<Long> timestamps = new Vector<Long>();

        boolean errorFlag = false;

        long n = -1;
        long fromAsLong = -1;
        long toAsLong = -1;

        if (size == null)
            n = DEFAULT_PREVIEW_SIZE;
        else
            try {
                n = Long.parseLong(size);
            } catch (NumberFormatException e) {
                logger.error(e.getMessage(), e);
            }

        if (n < 1) n = DEFAULT_PREVIEW_SIZE; // size should be strictly larger than 0

        if (from == null) { // no lower bound provided
            fromAsLong = getMinTimestampForSensorField(sensor, field);
        } else try {
            fromAsLong = new java.text.SimpleDateFormat(stringConstantsProperties.getProperty("ISO_FORMAT")).parse(from).getTime();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errorFlag = true;
        }

        if (to == null) { // no lower bound provided
            toAsLong = getMaxTimestampForSensorField(sensor, field);
        } else try {
            toAsLong = new java.text.SimpleDateFormat(stringConstantsProperties.getProperty("ISO_FORMAT")).parse(to).getTime();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errorFlag = true;
        }

        if (errorFlag) {
            return errorResponse(ErrorType.MALFORMED_DATE_FROM_TO, user, sensor);
        }

        errorFlag = !getDataPreview(sensor, field, fromAsLong, toAsLong, elements, timestamps, n);

        if (errorFlag) {
            return errorResponse(ErrorType.ERROR_IN_REQUEST, user, sensor);
        }
        
        VSensorConfig sensorConfig = Mappings.getConfig(sensor);
        VirtualSensor sensorObj = new VirtualSensor();
        
        sensorObj.setMetadata(createHeaderMap(sensorConfig));
        sensorObj.appendField(new DataField("time", "Time"));
        sensorObj.appendField(new DataField("timestamp", "Time"));
        for (DataField df: sensorConfig.getOutputStructure()){
        	if (df.getName().equals(field)){
        		 sensorObj.appendField(df);
        		 break;
        	}
        }
        
        sensorObj.setValues(elements, timestamps);

        List<VirtualSensor> listSens = new LinkedList<VirtualSensor>();
        listSens.add(sensorObj);

        restResponse.setResponse(VirtualSensor.generateFileContent(listSens, format));

        return restResponse;
    }

    
    
    
    //error handling
    public RestResponse errorResponse(ErrorType errorType, User user, String sensor) {
    	
    	String errorMessage = "";
        String filename = "";

        switch (errorType){
            case NO_SUCH_SENSOR:
                errorMessage = String.format(stringConstantsProperties.getProperty("ERROR_NO_SUCH_SENSOR_MSG"), sensor);
                filename = stringConstantsProperties.getProperty("ERROR_NO_SUCH_SENSOR_FILENAME");
                break;
            case NO_SUCH_USER:
                errorMessage = stringConstantsProperties.getProperty("ERROR_NO_SUCH_USER_MSG");
                filename = stringConstantsProperties.getProperty("ERROR_NO_SUCH_USER_FILENAME");
                break;
            case NO_SENSOR_ACCESS:
                errorMessage = String.format(stringConstantsProperties.getProperty("ERROR_NO_SENSOR_ACCESS_MSG"), user.getUserName(), sensor);
                filename = stringConstantsProperties.getProperty("ERROR_NO_SENSOR_ACCESS_FILENAME");
                break;
            case UNKNOWN_REQUEST:
                errorMessage = stringConstantsProperties.getProperty("ERROR_UNKNOWN_REQUEST_MSG");
                filename = stringConstantsProperties.getProperty("ERROR_UNKNOWN_REQUEST_FILENAME");
                break;
            case MALFORMED_DATE_FROM_TO:
                errorMessage = stringConstantsProperties.getProperty("ERROR_MALFORMED_DATE_FROM_TO_MSG");
                filename = stringConstantsProperties.getProperty("ERROR_MALFORMED_DATE_FROM_TO_FILENAME");
                break;
            case MALFORMED_DATE_DATE_FIELD:
                errorMessage = stringConstantsProperties.getProperty("ERROR_MALFORMED_DATE_DATE_FIELD_MSG");
                filename = stringConstantsProperties.getProperty("ERROR_MALFORMED_DATE_DATE_FIELD_FILENAME");
                break;
            case ERROR_IN_REQUEST:
                errorMessage = stringConstantsProperties.getProperty("ERROR_ERROR_IN_REQUEST_MSG");
                filename = stringConstantsProperties.getProperty("ERROR_ERROR_IN_REQUEST_FILENAME");
                break;
        }

    	
    	if (RestServlet.FORMAT_CSV.equals(format)) return errorResponseCSV(filename, errorMessage);
        else if (RestServlet.FORMAT_JSON.equals(format)) return errorResponseJSON(errorMessage);
        else return null;
    }
    
    private RestResponse errorResponseJSON(String errorMessage) {

        RestResponse restResponse = new RestResponse();

        JSONObject jsonObject = new JSONObject();
        
        jsonObject.put("error", errorMessage);

        restResponse.setResponse(jsonObject.toJSONString());
        restResponse.setHttpStatus(RestResponse.HTTP_STATUS_BAD_REQUEST);
        restResponse.setType(RestResponse.JSON_CONTENT_TYPE);

        return restResponse;
    }
    
    private RestResponse errorResponseCSV(String filename, String errorMessage) {

        RestResponse restResponse = new RestResponse();

        errorMessage = "# " + errorMessage;

        restResponse.setType(RestResponse.CSV_CONTENT_TYPE);
        restResponse.addHeader(RestResponse.RESPONSE_HEADER_CONTENT_DISPOSITION_NAME, String.format(RestResponse.RESPONSE_HEADER_CONTENT_DISPOSITION_VALUE, filename + ".csv"));
        restResponse.setResponse(errorMessage);
        restResponse.setHttpStatus(RestResponse.HTTP_STATUS_ERROR);

        return restResponse;
    }
    
    
    
    
    protected String datetime;

    protected static final long DEFAULT_PREVIEW_SIZE = 1000;

    public RequestHandler (String format) {
        //loading RestApiStringConstants properties file
        try {
            stringConstantsPropertiesFileInputStream = new FileInputStream(STRING_CONSTANTS_PROPERTIES_FILENAME);
            stringConstantsProperties.load(stringConstantsPropertiesFileInputStream);
        }
        catch (IOException ex){
            ex.printStackTrace();
            logger.error(ex.getMessage(), ex);
        }
        
        DateFormat dateFormat = new SimpleDateFormat(stringConstantsProperties.getProperty("DATE_FORMAT"));
        Date currentDate = Calendar.getInstance().getTime();
        datetime = dateFormat.format(currentDate);
        this.format = format;
    }

    //close input stream
    public void finish(){
        try {
            if (stringConstantsPropertiesFileInputStream != null) stringConstantsPropertiesFileInputStream.close();
        } catch (IOException ex){
            ex.printStackTrace();
            logger.error(ex.getMessage(), ex);
        }
    }

    
    
    
    //checking ac
    //if user has access to vs null is returned, otherwise RestResponse with error message
    private RestResponse userHasAccessToVirtualSensor(User user, String sensor){
        if (Mappings.getConfig(sensor) == null ) {
            return errorResponse(ErrorType.NO_SUCH_SENSOR, user, sensor);
        }
        if (Main.getContainerConfig().isAcEnabled()){
            if (user == null) {
                return errorResponse(ErrorType.NO_SUCH_USER, user, sensor);
            }
            if (!user.hasReadAccessRight(sensor) && !user.isAdmin() && DataSource.isVSManaged(sensor)) {
                return errorResponse(ErrorType.NO_SENSOR_ACCESS, user, sensor);
            }
        }
        return null;
    }

    //if user has access to vs null is returned, otherwise RestResponse with error message
    private RestResponse userExists(User user){
        if (Main.getContainerConfig().isAcEnabled() && (user == null)){
            return errorResponse(ErrorType.NO_SUCH_USER, user, null);
        }
        return null;
    }
    
    
    
    
    //helper methods
       
    private void setRestResponseParams(RestResponse restResponse, String filename){
    	if (RestServlet.FORMAT_CSV.equals(format)) {
    		restResponse.setType(RestResponse.CSV_CONTENT_TYPE);
            restResponse.addHeader(RestResponse.RESPONSE_HEADER_CONTENT_DISPOSITION_NAME, String.format(RestResponse.RESPONSE_HEADER_CONTENT_DISPOSITION_VALUE, filename + ".csv"));
    	}
        else if (RestServlet.FORMAT_JSON.equals(format)) {
        	restResponse.setType(RestResponse.JSON_CONTENT_TYPE);
        }
    	restResponse.setHttpStatus(RestResponse.HTTP_STATUS_OK);
    }

    private boolean getData(String sensor, String field, long from, long to, List<Vector<Double>> elements, Vector<Long> timestamps) {
        Connection conn = null;
        ResultSet resultSet = null;

        boolean result = true;

        try {
            conn = Main.getStorage(sensor).getConnection();
            StringBuilder query = new StringBuilder("select timed, ")
                    .append(field)
                    .append(" from ")
                    .append(sensor)
                    .append(" where timed >= ")
                    .append(from)
                    .append(" and timed<=")
                    .append(to);

            resultSet = Main.getStorage(sensor).executeQueryWithResultSet(query, conn);

            while (resultSet.next()) {
                Vector<Double> stream = new Vector<Double>();
                timestamps.add(resultSet.getLong(1));
                stream.add(getDouble(resultSet,field));
                elements.add(stream);
                
            }

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            result = false;
        } finally {
            Main.getStorage(sensor).close(resultSet);
            Main.getStorage(sensor).close(conn);
        }

        return result;
    }

    private boolean getDataPreview(String sensor, String field, long from, long to, List<Vector<Double>> elements, Vector<Long> timestamps, long size) {
        Connection conn = null;
        ResultSet resultSet = null;

        boolean result = true;

        long skip = getTableSize(sensor) / size;

        /*
        logger.warn("skip = " + skip);
        logger.warn("size = " + size);
        logger.warn("getTableSize(sensor) = " + getTableSize(sensor));
        */

        try {
            conn = Main.getStorage(sensor).getConnection();
            StringBuilder query = new StringBuilder("select timed, ")
                    .append(field)
                    .append(" from ")
                    .append(sensor);
            if (skip > 1)
                query.append(" where mod(pk,")
                        .append(skip)
                        .append(")=1");

            resultSet = Main.getStorage(sensor).executeQueryWithResultSet(query, conn);

            while (resultSet.next()) {
                //int ncols = resultSet.getMetaData().getColumnCount();
                long timestamp = resultSet.getLong(1);
                double value = resultSet.getDouble(2);
                //logger.warn(ncols + " cols, value: " + value + " ts: " + timestamp);
                Vector<Double> stream = new  Vector<Double>();
                stream.add(value);
                timestamps.add(timestamp);
                elements.add(stream);
            }

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            result = false;
        } finally {
            Main.getStorage(sensor).close(resultSet);
            Main.getStorage(sensor).close(conn);
        }

        return result;
    }

    private long getTableSize(String sensor) {
        Connection conn = null;
        ResultSet resultSet = null;

        boolean result = true;
        long timestamp = -1;

        try {
            conn = Main.getDefaultStorage().getConnection();
            StringBuilder query = new StringBuilder("select count(*) from ").append(sensor);

            resultSet = Main.getStorage(sensor).executeQueryWithResultSet(query, conn);

            if (resultSet.next()) {

                timestamp = resultSet.getLong(1);
            }

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            result = false;
        } finally {
            Main.getStorage(sensor).close(resultSet);
            Main.getStorage(sensor).close(conn);
        }

        return timestamp;
    }

    private Double getDouble(ResultSet rs,String fieldName) throws SQLException{
        Double d=rs.getDouble(fieldName);
        if (rs.wasNull()) return null;
            //if (o!=null) return rs.getDouble(fieldName);
        else return d;
    }

    private long getMinTimestampForSensorField(String sensor, String field) {
        return getTimestampBoundForSensorField(sensor, field, "min");
    }

    private long getMaxTimestampForSensorField(String sensor, String field) {
        return getTimestampBoundForSensorField(sensor, field, "max");
    }

    private long getTimestampBoundForSensorField(String sensor, String field, String boundType) {
        Connection conn = null;
        ResultSet resultSet = null;

        boolean result = true;
        long timestamp = -1;

        try {
            conn = Main.getDefaultStorage().getConnection();
            StringBuilder query = new StringBuilder("select ").append(boundType).append("(timed) from ").append(sensor);

            resultSet = Main.getStorage(sensor).executeQueryWithResultSet(query, conn);

            if (resultSet.next()) {

                timestamp = resultSet.getLong(1);
            }

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            result = false;
        } finally {
            Main.getStorage(sensor).close(resultSet);
            Main.getStorage(sensor).close(conn);
        }

        return timestamp;
    }
    
    private Map<String, String> createHeaderMap(VSensorConfig vsconf){
    	Map<String, String> metadata = new LinkedHashMap<String, String>();
    	metadata.put(stringConstantsProperties.getProperty("VS_NAME"), vsconf.getName());
    	if (Main.getContainerConfig().isAcEnabled()) metadata.put(stringConstantsProperties.getProperty("IS_PUBLIC"), new Integer((DataSource.isVSManaged(vsconf.getName())) ? 0 : 1).toString());
        for ( KeyValue df : vsconf.getAddressing()){
        	metadata.put(df.getKey().toString().toLowerCase().trim(), df.getValue().toString().trim());
        }
        return metadata;
	}

    
    
    
    
    //properties file with strings
    private static final String STRING_CONSTANTS_PROPERTIES_FILENAME = "conf/RestApiStringConstants.properties";
    private static Properties stringConstantsProperties = new Properties();
    private FileInputStream stringConstantsPropertiesFileInputStream= null;
 
    public static Properties getStringConstantsPropertiesFile(){
    	return stringConstantsProperties;
    }
}
