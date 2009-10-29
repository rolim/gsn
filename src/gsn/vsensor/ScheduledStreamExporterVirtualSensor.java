package gsn.vsensor;

import gsn.ContainerImpl;
import gsn.storage.StorageManager;
import gsn.utils.GSNRuntimeException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.TreeMap;
import java.util.TimerTask;

import org.apache.log4j.Logger;

/**
 * This virtual sensor saves its input stream to any JDBC accessible source.
 */
 public class ScheduledStreamExporterVirtualSensor extends AbstractScheduledVirtualSensor {

	public static final String            PARAM_USER    = "user" , PARAM_PASSWD = "password" , PARAM_URL = "url" , TABLE_NAME = "table",PARAM_DRIVER="driver";
	public static final String[] OBLIGATORY_PARAMS = new String[] {PARAM_USER,PARAM_URL,PARAM_DRIVER};

	private Connection                    connection;
	private CharSequence table_name;
	private String password;
	private String user;
	private String url;

	private static final transient Logger logger = Logger
			.getLogger(ScheduledStreamExporterVirtualSensor.class);
	
	public boolean initialize() {
		// Get the StreamExporter parameters
		TreeMap<String, String> params = getVirtualSensorConfiguration()
		.getMainClassInitialParams();
		for (String param : OBLIGATORY_PARAMS)
			if ( params.get( param ) == null || params.get(param).trim().length()==0) {
				logger.warn("Initialization Failed, The "+param+ " initialization parameter is missing");
				return false;
			}
		table_name = params.get( TABLE_NAME );
		user = params.get(PARAM_USER);
		password = params.get(PARAM_PASSWD);
		url = params.get(PARAM_URL);
		try {
			Class.forName(params.get(PARAM_DRIVER));
			connection = getConnection();
			logger.debug( "jdbc connection established." );
			if (!StorageManager.tableExists(table_name,getVirtualSensorConfiguration().getOutputStructure() , connection))
				StorageManager.executeCreateTable(table_name, getVirtualSensorConfiguration().getOutputStructure(), false,connection);
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage(),e);
			logger.error("Initialization of the Stream Exporter VS failed !");
			return false;
		} catch (SQLException e) {
			logger.error(e.getMessage(),e);
			logger.error("Initialization of the Stream Exporter VS failed !");
			return false;
		}catch (GSNRuntimeException e) {
			logger.error(e.getMessage(),e);
			logger.error("Initialization failed. There is a table called " + TABLE_NAME+ " Inside the database but the structure is not compatible with what GSN expects.");
			return false;
		}
		try {
			connection.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(),e);
		}
		super.initialize();   		//get the timer settings

		TimerTask timerTask = new MyTimerTask();
		timer0.scheduleAtFixedRate(timerTask, new Date(startTime), clock_rate);
		return true;
	}

	class MyTimerTask extends TimerTask {

		public void run() {
			
			if(dataItem == null)
				return;	
			
			dataItem.setTimeStamp(System.currentTimeMillis());
			logger.warn(getVirtualSensorConfiguration().getName() + " Timer Event ");
			StringBuilder query = StorageManager.getStatementInsert(table_name, getVirtualSensorConfiguration().getOutputStructure());
			
			try {
				connection = getConnection();

				StorageManager.executeInsert(table_name ,getVirtualSensorConfiguration().getOutputStructure(),dataItem,getConnection() );
				logger.warn(getVirtualSensorConfiguration().getName() + " Wrote to database ");
				connection.close();
			} catch (SQLException e) {
				logger.error(e.getMessage(),e);
				logger.error("Insertion failed! ("+ query+")");
			}finally {
			
				try {
					ContainerImpl.getInstance().publishData(ScheduledStreamExporterVirtualSensor.this, dataItem);
				} catch (SQLException e) {
					if (e.getMessage().toLowerCase().contains("duplicate entry"))
						logger.info(e.getMessage(), e);
					else
						logger.error(e.getMessage(), e);
				}
			}

		}
	}

	public Connection getConnection() throws SQLException {
		if (this.connection==null || this.connection.isClosed())
			this.connection=DriverManager.getConnection(url,user,password);
		return connection;
	}

	public void dispose() {
		timer0.cancel();
		try {
			this.connection.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(),e);
		}

		//<TODO> should we close the database connection here?
	}

}
