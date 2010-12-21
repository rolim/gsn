
package gsn.vsensor;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

import gsn.Main;
import gsn.Mappings;
import gsn.beans.Link;
import gsn.beans.NetworkTopology;
import gsn.beans.SensorNode;
import gsn.beans.SensorNodeConfiguration;
import gsn.beans.StreamElement;
import gsn.storage.DataEnumerator;

/**
* @author Roman Lim
*/

public class TopologyVirtualSensor extends AbstractVirtualSensor {
	
	private static final long LINK_TIMEOUT = 3600000;	   // time until a link is removed
	private static final long NODE_TIMEOUT = 7 * 24 * 3600000; // time until a node and its history is removed
	private static final long NODE_CONFIGURABLE_TIME = 5 * 60000; // time until a node is not configurable anymore
	private static final long NODE_CONFIGURE_TIMEOUT = 6 * 60000; // time to wait until configuration is resent
	private static final long NODE_CONFIGURE_NEXT_TRY_TIMEOUT = 30000; // time to wait until next configuration enry is tried
	private static final short EVENT_DATACONFIG = 40;
	private static final short EVENT_PSB_POWER = 32;
	private static final short EVENT_BB_POWER_OFF = 31;
	private static final short EVENT_BB_POWER_ON = 30;
	public static final short DATA_CONTROL_CMD = 1;
	public static final short NEW_DATA_CONTROL_CMD = 3;
	public static final short GUMSTIX_CTRL_CMD = 14;
	public static final short DATA_CONTROL_NETWORK_FLAG = 0x400;
	public static final short DATA_CONTROL_SENSOR_FLAG = 0x800;
	public static final short DATA_CONTROL_WRITE_FLAG = 0x800;
	public static final int BROADCAST_ADDR = 0xFFFF;
	private static short REPETITION_COUNT = 3;
	
	private final transient Logger logger = Logger.getLogger( this.getClass() );
	
	private static final String[] configurationParameters = {
		"node-id-field",
		"parent-id-field",
		"timestamp-field",
		"generation-time-field",
		"vsys-field",
		"current-field",
		"temperature-field",
		"humidity-field",
		"flash-count-field",
		"uptime-field",
		"access-node-stream-name",
		"powerswitch-stream-name",
		"rssi-stream-name", // this stream does not count to the packetcount
		"rssi-node-id-field",
		"rssi-field",
		"evenlogger-stream-name",// this stream does not count to the packetcount
		"evenlogger-id-field",
		"evenlogger-value-field",
		"valid-field",
		"powerswitch-p1-field",
		"powerswitch-p2-field",
		"sdivoltage-field",
	};
	
	private static final String commandConfigurationParameter = "dozer-command-vs";

	private String [] configuration = {};
	private String commandConfigurationValue;
	private Map<Integer, SensorNode> nodes;
	private CommandScheduler scheduler; 
	private boolean configurable;
	
	@Override
	synchronized public void dataAvailable(String inputStreamName, StreamElement data) {
		Integer node_id=null;
		Long timestamp=null;
		Long generation_time=null;
		Serializable s;
		boolean notifyscheduler=false;
		Short event_value = null;
		Short event_id = null;
		Integer node_type = null;
		Byte valid = null;
		Boolean p1 = null;
		Boolean p2 = null; 
		
		if ( logger.isDebugEnabled( ) ) logger.debug( new StringBuilder( "data received under the name *" ).append( inputStreamName ).append( "* to the TopologyVS." ).toString( ) );
		s = data.getData(configuration[0]);
		if (s instanceof Integer) {
			node_id = (Integer)s;
		}
		s = data.getData(configuration[2]);
		if (s instanceof Long) {
			timestamp = (Long)s;
		}
		s = data.getData(configuration[3]);
		if (s instanceof Long) {
			generation_time = (Long)s;
		}
		if (node_id==null || timestamp==null || generation_time==null) {
			logger.error("No node id specified, skipping stream element.");
			return;
		}
		synchronized (nodes) {
		if (!nodes.containsKey(node_id)) {
			logger.debug("new node: "+node_id);	
			nodes.put(node_id, new SensorNode(node_id)); 
		}
		SensorNode node = nodes.get(node_id);
		// save always latest timestamp
		if (node.timestamp==null || node.timestamp < timestamp) {
			node.timestamp = timestamp;
			node.generation_time = generation_time;
		}
		
		// RSSI
		if (inputStreamName.startsWith(configuration[12])) {
			logger.debug( "got rssi info:" + data.getData("HEADER_ORIGINATORID") + " " + data.getData("RSSI_NODEID") + " " + data.getData("RSSI"));
			Link newlink=null;
			Integer rssi_node_id;
			Short rssi;
			s = data.getData(configuration[13]);
			if (s instanceof Integer) {
				rssi_node_id = (Integer)s;
			}
			else {
				logger.debug("rssi node id wrong type");
				return;
			}
			s = data.getData(configuration[14]);
			if (s instanceof Short) {
				rssi = (Short)s;
			}
			else {
				logger.debug("rssi wrong type");
				return;
			}
			for (Iterator<Link> j = node.links.iterator(); j.hasNext();) { 
				Link l = j.next();
				if (l.node_id.equals(rssi_node_id)) {
					newlink = l;
					break;
				}
			}
			if (newlink == null) {
				newlink = new Link();
				node.links.add(newlink);
			}
			newlink.node_id=rssi_node_id;
			newlink.rssi=rssi;
			newlink.timestamp=timestamp;
			// do not count rssi info to packets
		}
		// events
		else if (inputStreamName.startsWith(configuration[15])) {
			// eventlogger
			s = data.getData(configuration[16]);
			if (s instanceof Short) {
				event_id = (Short)s;
			}
			s = data.getData(configuration[17]);
			if (s instanceof Integer) {
				event_value = ((Integer)s).shortValue();
			}
			logger.debug("got event "+event_id+" with value "+(event_value!=null?event_value:"null"));
			if (event_id == EVENT_DATACONFIG && event_value!=null) {
				logger.debug("added data configuration");
				if (node.configuration==null)
					node.configuration = new SensorNodeConfiguration(event_value, node.nodetype, timestamp);
				else
					node.configuration.update(event_value, node.nodetype);
				if (node.pendingConfiguration!=null) {
					if (node.pendingConfiguration.hasDataConfig() && node.configuration.getConfiguration().equals(node.pendingConfiguration.getConfiguration()))
						node.pendingConfiguration.removeDataConfig();
					if (!node.pendingConfiguration.hasDataConfig() && !node.pendingConfiguration.hasPortConfig())
						node.pendingConfiguration=null;
				}
				notifyscheduler=true;
				node_type = new Integer(node.nodetype);
			}
			else if (event_id == EVENT_PSB_POWER && event_value!=null) {
				p1 = (event_value & 1) > 0;
				p2 = (event_value & 2) > 0;
				logger.debug("received port info (event): "+p1+" "+p2);
				if (node.pendingConfiguration!=null) {
					logger.debug("have pending "+node.pendingConfiguration.powerswitch_p1+" "+node.pendingConfiguration.powerswitch_p2);
					if (node.pendingConfiguration.hasPortConfig() && node.pendingConfiguration.powerswitch_p1.equals(p1) && node.pendingConfiguration.powerswitch_p2.equals(p2)) {
						node.pendingConfiguration.removePortConfig();
						logger.debug("remove port config");
					}
					if (!node.pendingConfiguration.hasDataConfig() && !node.pendingConfiguration.hasPortConfig()) {
						node.pendingConfiguration=null;
						logger.debug("remove pending");
					}
					else
						logger.debug("config still pending: "+node.pendingConfiguration.hasDataConfig()+" "+node.pendingConfiguration.hasPortConfig());
				}
				notifyscheduler=true;
				node_type = new Integer(node.nodetype);
			}
			else if (event_id == EVENT_BB_POWER_ON || event_id == EVENT_BB_POWER_OFF) {
				node.setBBControl();
				if (event_id == EVENT_BB_POWER_ON)
					node.corestation_running = new Boolean(true);
				else
					node.corestation_running = new Boolean(false);
			}
			// do not count events to packets
		}
		else {
		node.packet_count++;
		if (inputStreamName.equals(configuration[10]) && !node.isAccessNode()) {
			node.setAccessNode();
			// adjust configuration
			node.pendingConfiguration=null;
			if (node.configuration!=null)
				node.configuration=new SensorNodeConfiguration(node.configuration, node.nodetype);
		}
		else if (inputStreamName.equals(configuration[11])) { // power switch packets
			if (!node.isPowerSwitch()) {
				node.setPowerSwitch();
				// 	adjust configuration
				node.pendingConfiguration=null;
				if (node.configuration!=null)
					node.configuration=new SensorNodeConfiguration(node.configuration, node.nodetype);
			}
			s = data.getData(configuration[19]);
			if (s instanceof Byte)
				p1 = ((Byte)s == 1);
			s = data.getData(configuration[20]);
			if (s instanceof Byte)
				p2 = ((Byte)s == 1);
			if (p1!=null && p2!=null) {
				if (node.configuration==null)
					node.configuration=new SensorNodeConfiguration();
				node.configuration.update(p1, p2);
				logger.debug("received port info: "+p1+" "+p2);
				if (node.pendingConfiguration!=null) {
					if (node.pendingConfiguration.hasPortConfig())
						logger.debug("pending config: "+node.pendingConfiguration.powerswitch_p1+" "+node.pendingConfiguration.powerswitch_p2);
					if (node.pendingConfiguration.hasPortConfig() && node.pendingConfiguration.powerswitch_p1.equals(p1) && node.pendingConfiguration.powerswitch_p2.equals(p2)) {
						node.pendingConfiguration.removePortConfig();
					}
					if (!node.pendingConfiguration.hasDataConfig() && !node.pendingConfiguration.hasPortConfig())
						node.pendingConfiguration=null;
				}
				notifyscheduler=true;
				event_id = EVENT_PSB_POWER;
				node_type = new Integer(node.nodetype);
			}
		}
		s = data.getData(configuration[1]);
		if (s instanceof Integer) {
			node.parent_id = (Integer)s;
		}

		// health
		// save always latest health information
		if (node.timestamp == timestamp) {
			s = data.getData(configuration[4]);
			if (s instanceof Integer) {
				if (node.isSibNode()) {
					node.setVsys(new Double((Integer)s)  * (2.56d/65536d) * (39d/24d));
				}
				else if (node.isPowerSwitch()) {
					node.setVsys(new Double((Integer)s)  * (2.5d / 4095d) * (8d/5d));
				} 
				else if (node.isAccessNode()) {
					node.setVsys(new Double((Integer)s)  * (3d / 4095d));
				}
			}
			s = data.getData(configuration[5]);
			if (s instanceof Integer) {
				if ((Integer)s==0xffff || !node.isSibNode())
					node.current=null;
				else
					node.current = new Double((Integer)s) * 2.56 / Math.pow(2, 16) / 0.15 * 10;
			}
			s = data.getData(configuration[18]); // valid
			if (s instanceof Byte) {
				valid = (Byte)s;
				logger.debug("valid is "+valid);
			}
			s = data.getData(configuration[6]);
			if (s instanceof Integer) {
				if ((Integer)s==0xffff || (valid!=null && valid==0)) {
					node.temperature = null;
					node.humidity = null;
				}
				else {
					node.temperature = new Double(-39.63d + (0.01d * (new Double((Integer)s))));
					s = data.getData(configuration[7]);
					if (s instanceof Integer) {
						if ((Integer)s==0xffff)
							node.humidity = null;
						else {
							Double d = new Double((Integer)s);
							Double hum_rel = new Double(-4 + (0.0405d * d) - 0.0000028d * Math.pow(d, 2));				
							node.humidity = new Double((node.temperature - 25) * (0.01d + (0.00008d * d)) + hum_rel);
						}
					}
				}
			}
			s = data.getData(configuration[8]);
			if (s instanceof Integer)
				node.flash_count = (Integer)s;
			s = data.getData(configuration[9]);
			if (s instanceof Integer)
				node.uptime = (Integer)s;
			s = data.getData(configuration[21]);
			if (s instanceof Integer) {
				if (node.isBBControl()) {
					logger.debug("BBControl system voltage: "+(new Double((Integer)s)  * (2.5d / 4095d) * (115d/15d))+"("+s+")");
					node.setVsys(new Double((Integer)s)  * (2.5d / 4095d) * (115d/15d));
				}
			}
		}
		}
		// remove outdated information
		Long now = System.currentTimeMillis();
		for (Iterator<SensorNode> i = nodes.values().iterator(); i.hasNext();) {
			SensorNode n = i.next();
			if (now - n.timestamp > NODE_TIMEOUT) {
				logger.debug("remove node "+n.node_id+", last timestamp was "+n.timestamp);
				i.remove();	
			}
			else {
				for (Iterator<Link> j = n.links.iterator(); j.hasNext();) { 
					Link l = j.next();
					if (now - l.timestamp > LINK_TIMEOUT) {
						logger.debug("remove link from "+n.node_id+" to "+l.node_id+", last timestamp was "+l.timestamp);
						j.remove();
					}
				}
			}
		}
		generateData();
		}
		// notify scheduler
		if (notifyscheduler && scheduler!=null) {
			logger.debug("notify scheduler");
			if (event_id == EVENT_DATACONFIG)
				scheduler.configurationUpdate(node_id, event_value, node_type);
			else if (p1!=null && p2!=null)
				scheduler.portConfigurationUpdate(node_id, p1, p2, node_type);
		}

	}
	
	private void addToQueue(Integer node_id, Integer nodetype, SensorNodeConfiguration c, List<SensorNode> queue) {
		if (c.hasDataConfig()) {
			logger.debug("add data config to queue for node "+node_id);
			SensorNode newnode = new SensorNode(node_id);
			newnode.pendingConfiguration = new SensorNodeConfiguration(c, nodetype);
			newnode.pendingConfiguration.removePortConfig();
			newnode.pendingConfiguration.timestamp = System.currentTimeMillis();
			queue.add(newnode);
		}
		if (c.hasPortConfig()) {
			logger.debug("add port config to queue for node "+node_id);
			SensorNode newnode = new SensorNode(node_id);
			newnode.pendingConfiguration = new SensorNodeConfiguration(c, nodetype);
			newnode.pendingConfiguration.removeDataConfig();
			newnode.pendingConfiguration.timestamp = System.currentTimeMillis();
			queue.add(newnode);
		}
	}
	
	@Override
	synchronized public boolean dataFromWeb ( String action,String[] paramNames, Serializable[] paramValues ) {
		ArrayList<SensorNode> configurationQueue;
		// read new network configuration
		int index = Arrays.asList(paramNames).indexOf("configuration");
		if (index < 0) {
			logger.debug("field <configuration> not found.");
			return false;
		}
		logger.debug("trying to parse configuration.");
		IBindingFactory bfact;
		try {
			Serializable s = paramValues[index];
			if (s instanceof String) {
				bfact = BindingDirectory.getFactory(NetworkTopology.class);
				IUnmarshallingContext uctx = bfact.createUnmarshallingContext();		
				NetworkTopology parsedconfiguration = (NetworkTopology) uctx.unmarshalDocument(new ByteArrayInputStream(
						((String)s).getBytes()), "UTF-8");
				configurationQueue = new ArrayList<SensorNode>();
				synchronized (nodes) {
					// remove all pending configurations
					// are there any missing configurations ?
					boolean queryNetwork = false;
					for (SensorNode n: nodes.values()) {
						n.pendingConfiguration=null;
						if (n.configuration==null && (System.currentTimeMillis() - n.generation_time < NODE_CONFIGURABLE_TIME) && !n.isAccessNode())
							queryNetwork=true;
					}
					if (queryNetwork) {
						logger.debug("enqueue query network command");
						SensorNode newnode = new SensorNode(BROADCAST_ADDR);
						configurationQueue.add(newnode);
					}
					for (SensorNode n: parsedconfiguration.sensornodes) {
						// compare with current configuration
						SensorNode cn = nodes.get(n.node_id);
						if (n.node_id !=null && n.configuration!=null 
								&& cn!=null
								&& (
										cn.configuration==null
										|| !n.configuration.equals(cn.configuration))
						){
							logger.debug("new configuration for node "+n.node_id);
							// always enable events
							n.configuration.events = true;
							// always enable health
							n.configuration.health = true;
							cn.pendingConfiguration = new SensorNodeConfiguration(
									n.configuration,
									cn.nodetype);
							logger.debug("old config "+ cn.configuration + " "+cn.configuration.getConfiguration());
							logger.debug("new config "+ cn.pendingConfiguration + " "+cn.pendingConfiguration.getConfiguration());
							if (cn.pendingConfiguration.hasDataConfig() && cn.pendingConfiguration.getConfiguration().equals(cn.configuration.getConfiguration())) {
								cn.pendingConfiguration.removeDataConfig();
								logger.debug("same data config");
							}
							if (cn.pendingConfiguration.hasPortConfig() && cn.pendingConfiguration.getPortConfiguration().equals(cn.configuration.getPortConfiguration())) {
								cn.pendingConfiguration.removePortConfig();
								logger.debug("same port config");
							}
							if (!cn.pendingConfiguration.hasPortConfig() && !cn.pendingConfiguration.hasDataConfig())
								cn.pendingConfiguration=null;
							if (cn.pendingConfiguration!=null) {
								// add to queue
								addToQueue(cn.node_id, cn.nodetype, cn.pendingConfiguration, configurationQueue);
								cn.pendingConfiguration.timestamp = System.currentTimeMillis();
							}
						}
					}
					generateData();
				}
				logger.info("successfully read new configuration.");

			}
			else {
				logger.warn("data type was "+s.getClass().getCanonicalName());
				return false;
			}
		} catch (JiBXException e) {
			logger.error("unmarshall did fail: "+e);
			return false;
		}
		// schedule reconfigure commands
		if(configurationQueue.size()>0 && scheduler!=null) {
			scheduler.reschedule(configurationQueue);
		}
		return true;
	}
	
	synchronized void generateData() {
		NetworkTopology net = new NetworkTopology(configurable); 
		net.sensornodes = (SensorNode[])nodes.values().toArray(new SensorNode[nodes.size()]);
		try {
			IBindingFactory bfact = BindingDirectory.getFactory(NetworkTopology.class);
			IMarshallingContext mctx = bfact.createMarshallingContext();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			mctx.marshalDocument(net, "UTF-8", null, baos);
			StreamElement se = new StreamElement(getVirtualSensorConfiguration().getOutputStructure(),  new Serializable[]{baos.toString().getBytes()});
			dataProduced( se );
		} catch (JiBXException e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

	@Override
	public void dispose() {
		if (scheduler != null)
			scheduler.shutdown();
	}

	@Override
	public boolean initialize() {
		ArrayList<SensorNode> configurationQueue = new ArrayList<SensorNode>();
		TreeMap <  String , String > params = getVirtualSensorConfiguration( ).getMainClassInitialParams( );
		// check if all parameters are specified
		ArrayList<String> c = new ArrayList<String>();
		for (String s : configurationParameters) {
			String val = params.get(s);
			if (val == null) {
				logger.error("Could not initialize, missing parameter "+s);
				return false;
			}
			c.add(val);
		}
		configuration = c.toArray(configuration);
		commandConfigurationValue = params.get(commandConfigurationParameter);
		if (commandConfigurationValue!=null) {
			commandConfigurationValue = commandConfigurationValue.toLowerCase();
			logger.debug("start command scheduler");
			scheduler = new CommandScheduler(commandConfigurationValue, this);
			scheduler.start();
			configurable = true;
		}
		nodes = new Hashtable<Integer, SensorNode>();
		// load last available topology information
		String virtual_sensor_name = getVirtualSensorConfiguration().getName();
		StringBuilder query=  new StringBuilder("select * from " ).append(virtual_sensor_name).append(" where timed = (select max(timed) from " ).append(virtual_sensor_name).append(") order by PK desc limit 1");
		ArrayList<StreamElement> latestvalues=new ArrayList<StreamElement>() ;
		try {
	      DataEnumerator result = Main.getStorage(virtual_sensor_name).executeQuery( query , false );
	      while ( result.hasMoreElements( ) ) 
	    	  latestvalues.add(result.nextElement());
	    } catch (SQLException e) {
	      logger.error("ERROR IN EXECUTING, query: "+query);
	      logger.error(e.getMessage(),e);
	    }
	    if (latestvalues.size()>0) {
	    	IBindingFactory bfact;
			try {
				Serializable s = latestvalues.get(latestvalues.size()-1).getData()[0];
				if (s instanceof byte[]) {
					bfact = BindingDirectory.getFactory(NetworkTopology.class);
					IUnmarshallingContext uctx = bfact.createUnmarshallingContext();		
					NetworkTopology lastTopology = (NetworkTopology) uctx.unmarshalDocument(new ByteArrayInputStream(
						(byte[])s), "UTF-8");
					for (SensorNode n: lastTopology.sensornodes) {
						if (n.node_id !=null && n.timestamp != null && n.generation_time != null) {
							if (n.configuration!=null && n.pendingConfiguration!=null) {
								if (n.pendingConfiguration.hasDataConfig() && n.pendingConfiguration.getConfiguration().equals(n.configuration.getConfiguration()))
									n.pendingConfiguration.removeDataConfig();
								if (n.pendingConfiguration.hasPortConfig() && n.pendingConfiguration.getPortConfiguration().equals(n.configuration.getPortConfiguration()))
									n.pendingConfiguration.removePortConfig();
								if (!n.pendingConfiguration.hasPortConfig() && !n.pendingConfiguration.hasDataConfig())
									n.pendingConfiguration=null;
								if (n.pendingConfiguration!=null) {
									// add to queue
									addToQueue(n.node_id, n.nodetype, n.pendingConfiguration, configurationQueue);
								}
							}
							nodes.put(n.node_id, n);
						}
					}
					logger.info("successfully imported last network topology.");
				}
				else {
					logger.warn("data type was "+s.getClass().getCanonicalName());
				}
			} catch (JiBXException e) {
				logger.error("unmarshall did fail: "+e);
			}
	    }
	    else {
	    	logger.info("no old network status found.");
	    }

		if (configurationQueue.size()>0 && scheduler!=null) {
			scheduler.reschedule(configurationQueue);
		}	    
		return true;
	}
	
	protected boolean isOnline (Integer node_id) {
		synchronized (nodes) {
			return nodes.containsKey(node_id) && System.currentTimeMillis() - nodes.get(node_id).generation_time < NODE_CONFIGURABLE_TIME; 
		}
	}
	
	class CommandScheduler extends Thread {

		private volatile boolean running=false;
		private List<SensorNode> queue;
		private List<SensorNode> newqueue;
		private String CommandVSName;
		private SensorNode currentNode;
		private boolean rescheduled;
		private boolean configurationdone;
		private TopologyVirtualSensor tvs;
		private boolean isDataConfig;
		
		public CommandScheduler(String CommandVSName, TopologyVirtualSensor tvs) {
			this.CommandVSName = CommandVSName;
			this.tvs = tvs;
			setName("CommandScheduler");
			queue = new ArrayList<SensorNode>();
			logger.debug("use vs "+CommandVSName+" for commands");
		}
		
		synchronized public void reschedule(Iterable<SensorNode> network) {
			newqueue = new ArrayList<SensorNode>();
			for (Iterator<SensorNode> i=network.iterator();i.hasNext();) {
				SensorNode n = i.next();
				if (n.pendingConfiguration != null || n.node_id.equals(BROADCAST_ADDR)) {
					newqueue.add(n);
				}
			}
			logger.debug("reschedule, "+newqueue.size()+" entries");
			synchronized (this) {
				rescheduled = true;
				this.notify();
			}
		}
		
		private void sendDataConfigCommand(Integer destination, SensorNodeConfiguration config) {
			AbstractVirtualSensor vs;
			String[] fieldnames = {"destination","cmd","arg","repetitioncnt"};
			Serializable[] values =  {destination.toString(), Short.toString(DATA_CONTROL_CMD), config==null?"0":(Short.toString((short)(config.getConfiguration() + DATA_CONTROL_NETWORK_FLAG + DATA_CONTROL_SENSOR_FLAG))), Short.toString(REPETITION_COUNT)};
			// old or new config command ?
			if (config != null && config.vaisala_wxt520) {
				values[1]=Short.toString(NEW_DATA_CONTROL_CMD);
				values[2]= config==null?"0":(Short.toString((short)(config.getConfiguration() + DATA_CONTROL_WRITE_FLAG)));
			}
			logger.debug(CommandVSName+"< dest: "+destination +" data config: "+(config==null?"null":config.getConfiguration()));
			try {
				vs = Mappings.getVSensorInstanceByVSName(CommandVSName).borrowVS();
				logger.debug("send command via "+vs.getVirtualSensorConfiguration().getName());
				vs.dataFromWeb("tosmsg", fieldnames, values);
				Mappings.getVSensorInstanceByVSName(CommandVSName).returnVS(vs);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e);
			}
		}
		
		private void sendPortConfigCommand(Integer destination, SensorNodeConfiguration config) {
			AbstractVirtualSensor vs;
			String[] fieldnames = {"destination","cmd","arg","repetitioncnt"};
			Serializable[] values =  {destination.toString(), Short.toString(GUMSTIX_CTRL_CMD), Short.toString((short)(config.getPortConfiguration())), Short.toString(REPETITION_COUNT)};
			logger.debug(CommandVSName+"< dest: "+destination +" port config: "+config.getPortConfiguration());
			try {
				vs = Mappings.getVSensorInstanceByVSName(CommandVSName).borrowVS();
				logger.debug("send command via "+vs.getVirtualSensorConfiguration().getName());
				vs.dataFromWeb("tosmsg", fieldnames, values);
				Mappings.getVSensorInstanceByVSName(CommandVSName).returnVS(vs);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e);
			}
		}
		
		public void configurationUpdate(Integer node_id, Short event_value, Integer node_type) {
			synchronized (this) {
				logger.debug("configurationUpdate: "+node_id+" "+event_value+" "+node_type);
				if (currentNode!=null && node_id.equals(currentNode.node_id)) {
					logger.debug("received configuration update from current node ["+node_id+"]");
					SensorNodeConfiguration testconfig = new SensorNodeConfiguration(event_value, node_type);
					if (isDataConfig && currentNode.pendingConfiguration.getConfiguration().equals(testconfig.getConfiguration())) {
						logger.debug("data configuration done");
						configurationdone = true;
						this.notify();	
					}
				}
				else {
					logger.debug("not current node "+(currentNode==null?"null":currentNode.node_id));
					for (Iterator<SensorNode> i=queue.iterator();i.hasNext();){
						SensorNode n = i.next();
						if (n.node_id.equals(node_id) && n.pendingConfiguration.hasDataConfig()) {
							SensorNodeConfiguration testconfig = new SensorNodeConfiguration(event_value, node_type);
							if (n.pendingConfiguration.getConfiguration().equals(testconfig.getConfiguration())) {
								n.pendingConfiguration.removeDataConfig();
								if (!n.pendingConfiguration.hasPortConfig()) {
									logger.debug("remove pending config for node "+node_id);
									i.remove();
								}
							}
						}
					}
				}
			}
		}
		
		public void portConfigurationUpdate(Integer node_id, Boolean p1, Boolean p2, Integer node_type) {
			synchronized (this) {
				logger.debug("port configurationUpdate: "+node_id+" "+p1+" "+p2+" "+node_type);				
				if (currentNode!=null && node_id.equals(currentNode.node_id)) {
					logger.debug("received configuration update from current node ["+node_id+"]");
					SensorNodeConfiguration testconfig = new SensorNodeConfiguration(p1, p2);
					if (!isDataConfig && currentNode.pendingConfiguration.getPortConfiguration().equals(testconfig.getPortConfiguration())) {
						logger.debug("port configuration done");
						configurationdone = true;
						this.notify();	
					}
				}
				else {
					logger.debug("not current node "+(currentNode==null?"null":currentNode.node_id));
					for (Iterator<SensorNode> i=queue.iterator();i.hasNext();){
						SensorNode n = i.next();
						if (n.node_id.equals(node_id) && n.pendingConfiguration.hasPortConfig()) {
							SensorNodeConfiguration testconfig = new SensorNodeConfiguration(p1, p2);
							if (n.pendingConfiguration.getPortConfiguration().equals(testconfig.getPortConfiguration())) {
								n.pendingConfiguration.removePortConfig();
								if (!n.pendingConfiguration.hasDataConfig()) {
									logger.debug("remove pending config for node "+node_id);
									i.remove();
								}
							}
						}
					}
				}
			}
		}

		@Override
		public void run() {
			running=true;
			long timeout;
			try {
				synchronized (this) {
					while (running) {
						if (queue.size()==0) {
							logger.debug("suspend scheduler");
							this.wait();
						}
						logger.debug("unsuspend scheduler");
						if (rescheduled) {
							rescheduled = false;
							queue = newqueue;							
						}
						if (!running)
							break;
						// node online?
						logger.debug("queue has "+queue.size()+" entries");
						for (Iterator<SensorNode> i = queue.iterator();i.hasNext();) {
							SensorNode n=i.next();
							if (n.node_id.equals(BROADCAST_ADDR)) {
								currentNode = n;
								configurationdone = true;
							}
							else if (tvs.isOnline(n.node_id)) {
								currentNode = n;
								configurationdone = false;
								break;
							}
							else {
								logger.debug("node "+n.node_id+" is not online");
							}
						}
						if (currentNode != null) {
							queue.remove(currentNode);
							queue.add(currentNode);
							// send command
							if (currentNode.node_id.equals(BROADCAST_ADDR)) {
								logger.debug("send query to "+currentNode.node_id);
								sendDataConfigCommand(currentNode.node_id, null);
							}
							else if (currentNode.pendingConfiguration.hasDataConfig()) {
								logger.debug("send data configuration to "+currentNode.node_id);
								isDataConfig = true;
								sendDataConfigCommand(currentNode.node_id, currentNode.pendingConfiguration);
							}
							else {
								logger.debug("send port configuration to "+currentNode.node_id);
								isDataConfig = false;
								sendPortConfigCommand(currentNode.node_id, currentNode.pendingConfiguration);
							}
							timeout = NODE_CONFIGURE_TIMEOUT;
						}
						else
							timeout = NODE_CONFIGURE_NEXT_TRY_TIMEOUT;
						// wait
						logger.debug("wait");
						this.wait(timeout);
						logger.debug("timeout done");
						if (rescheduled)
							continue;
						if (currentNode!=null) {
							if (configurationdone) {
								// configuration was success-full
								logger.debug("configuration done for node "+currentNode.node_id);
								//remove from queue
								queue.remove(currentNode);
							}
							else {
								logger.debug("configuration failed for node "+currentNode.node_id);
							}
						}
						currentNode = null;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e);
			}	
			logger.debug("command scheduler ended");
		}	
		
		public void shutdown() {
			running = false;
			synchronized (this) {
				this.notifyAll();
			}
		}
	}

}