package edu.wisc.cs.sdn.apps.l3routing;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import edu.wisc.cs.sdn.apps.util.SwitchCommands;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.wisc.cs.sdn.apps.util.Host;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;

public class L3Routing implements IFloodlightModule, IOFSwitchListener, 
		ILinkDiscoveryListener, IDeviceListener
{
	public static final String MODULE_NAME = L3Routing.class.getSimpleName();
	
	// Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;

    // Interface to link discovery service
    private ILinkDiscoveryService linkDiscProv;

    // Interface to device manager service
    private IDeviceService deviceProv;
    
    // Switch table in which rules should be installed
    public static byte table;
    
    // Map of hosts to devices
    private Map<IDevice,Host> knownHosts;

	/**
     * Loads dependencies and initializes data structures.
     */
	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		Map<String,String> config = context.getConfigParams(this);
        table = Byte.parseByte(config.get("table"));
        
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
        this.linkDiscProv = context.getServiceImpl(ILinkDiscoveryService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        
        this.knownHosts = new ConcurrentHashMap<IDevice,Host>();
	}

	/**
     * Subscribes to events and performs other startup tasks.
     */
	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException
	{
		log.info(String.format("Starting %s...", MODULE_NAME));
		this.floodlightProv.addOFSwitchListener(this);
		this.linkDiscProv.addListener(this);
		this.deviceProv.addListener(this);
		
		/*********************************************************************/
		/* TODO: Initialize variables or perform startup tasks, if necessary */
		/*********************************************************************/
	}

	
    /**
     * Get a list of all known hosts in the network.
     */
    private Collection<Host> getHosts()
    { return this.knownHosts.values(); }
	
    /**
     * Get a map of all active switches in the network. Switch DPID is used as
     * the key.
     */
	private Map<Long, IOFSwitch> getSwitches()
    { return floodlightProv.getAllSwitchMap(); }
	
    /**
     * Get a list of all active links in the network.
     */
    private Collection<Link> getLinks()
    { return linkDiscProv.getLinks().keySet(); }

    /**
     * Event handler called when a host joins the network.
     * @param device information about the host
     */
	@Override
	public void deviceAdded(IDevice device) 
	{
		Host host = new Host(device, this.floodlightProv);
		// We only care about a new host if we know its IP
		if (host.getIPv4Address() != null)
		{
			log.info(String.format("Host %s added", host.getName()));
			this.knownHosts.put(device, host);
			
			/*****************************************************************/
			/* TODO: Update routing: add rules to route to new host          */
			this.unistallAllRules();
			this.installRules();
			/*****************************************************************/
		}
	}

	/**
     * Event handler called when a host is no longer attached to a switch.
     * @param device information about the host
     */
	@Override
	public void deviceRemoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{ return; }
		this.knownHosts.remove(device);

		log.info(String.format("Host %s is no longer attached to a switch", 
				host.getName()));
		
		/*********************************************************************/
		/* TODO: Update routing: remove rules to route to host               */
		this.uninstallRules(host);
		/*********************************************************************/
	}

	/**
     * Event handler called when a host moves within the network.
     * @param device information about the host
     */
	@Override
	public void deviceMoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{
			host = new Host(device, this.floodlightProv);
			this.knownHosts.put(device, host);
		}
		
		if (!host.isAttachedToSwitch())
		{
			this.deviceRemoved(device);
			return;
		}
		log.info(String.format("Host %s moved to s%d:%d", host.getName(),
				host.getSwitch().getId(), host.getPort()));
		
		/*********************************************************************/
		/* TODO: Update routing: change rules to route to host               */
		this.unistallAllRules();
		this.installRules();
		/*********************************************************************/
	}
	
    /**
     * Event handler called when a switch joins the network.
     * @param DPID for the switch
     */
	@Override		
	public void switchAdded(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d added", switchId));
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		this.unistallAllRules();
		this.installRules();
		/*********************************************************************/
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d removed", switchId));
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		this.unistallAllRules();
		this.installRules();
		/*********************************************************************/
	}

	/**
	 * Event handler called when multiple links go up or down.
	 * @param updateList information about the change in each link's state
	 */
	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) 
	{
		for (LDUpdate update : updateList)
		{
			// If we only know the switch & port for one end of the link, then
			// the link must be from a switch to a host
			if (0 == update.getDst())
			{
				log.info(String.format("Link s%s:%d -> host updated", 
					update.getSrc(), update.getSrcPort()));
			}
			// Otherwise, the link is between two switches
			else
			{
				log.info(String.format("Link s%s:%d -> s%s:%d updated", 
					update.getSrc(), update.getSrcPort(),
					update.getDst(), update.getDstPort()));
			}
		}
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		this.unistallAllRules();
		this.installRules();
		/*********************************************************************/
	}

	/** Install rules for all hosts */
	private void installRules(){
		Map<Long, Map<Long, Link>> bestRoutes = this.updateShortestPath();
		Collection<Host> hosts = this.getHosts();
		Map<Long, IOFSwitch> switches = this.getSwitches();
		for (Host h1 : hosts){
			if (!h1.isAttachedToSwitch()) continue;
			this.installRule(h1.getSwitch(), h1, h1.getPort());
			for (Host h2: hosts){
				if (h1.equals(h2) || !h2.isAttachedToSwitch()) continue;
				this.installRule(h2.getSwitch(), h2, h2.getPort());
				IOFSwitch s1 = h1.getSwitch();
				IOFSwitch s2 = h2.getSwitch();
				while (!s2.equals(s1)) {
					Link link = bestRoutes.get(s2.getId()).get(s1.getId());
					if (null == link) {
						System.out.println("Graph is not connected");
						break;
					}
					int port = link.getSrcPort();
					this.installRule(s2, h1, port);
					s2 = switches.get(link.getDst());
				}
			}
		}
	}

	/** Install rules for one host, to be used when host is added or moved.
	private void installRules(Host h1){
		if (!h1.isAttachedToSwitch()) return;
		this.installRule(h1.getSwitch(), h1, h1.getPort());
		Collection<Host> hosts = this.getHosts();
		Map<Long, IOFSwitch> switches = this.getSwitches();
		for (Host h2 : hosts) {
			if (h1.equals(h2) || !h2.isAttachedToSwitch()) continue;
			this.installRule(h2.getSwitch(), h2, h2.getPort());
			IOFSwitch s1 = h1.getSwitch();
			IOFSwitch s2 = h2.getSwitch();
			while (!s1.equals(s2)) {
				Link link = this.bestPaths.get(s1.getId()).get(s2.getId());
				if (null == link){
					System.out.println("Link is null");
					continue;
				}
				int port = link.getSrcPort();
				this.installRule(s1, h2, port);
				s1 = switches.get(link.getDst());
			}
		}
	}*/

	/** Install one rule for a (switch, host) */
	private void installRule(IOFSwitch s, Host h, int port){
		if (!h.isAttachedToSwitch()) { return; }
		OFMatch match = new OFMatch();
		match.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
		match.setNetworkDestination(h.getIPv4Address());
		OFAction action = new OFActionOutput(port);
		OFInstruction ins = new OFInstructionApplyActions(Arrays.asList(action));
		SwitchCommands.installRule(s, this.table, SwitchCommands.DEFAULT_PRIORITY, match, Arrays.asList(ins));
	}

	/** Uninstall rules on all switches */
	private void unistallAllRules(){
		for (Host h : this.getHosts()){
			this.uninstallRules(h);
		}
	}

	/** Remove rules for one host for all switches */
	private void uninstallRules(Host h){
		Map<Long, IOFSwitch> switches = this.getSwitches();
		for (IOFSwitch s : switches.values()){
			this.uninstallRule(s, h);
		}
	}

	/** Remove one rule for a (switch, host) */
	private void uninstallRule(IOFSwitch s, Host h){
		OFMatch match = new OFMatch();
		match.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
		match.setNetworkDestination(h.getIPv4Address());
		SwitchCommands.removeRules(s, this.table, match);
	}

	/** Update bestRoutes and bestPaths
	 *  using Floyd-Warshall algorithm */
	private Map<Long, Map<Long, Link>> updateShortestPath(){
		Map<Long, Map<Long, Integer>> bestDists = new HashMap<Long, Map<Long, Integer>>();
		Map<Long, Map<Long, Link>> bestRoutes = new HashMap<Long, Map<Long, Link>>();
		Set<Long> switches = this.getSwitches().keySet();
		for (Long i : switches){
			Map<Long, Link> linkMap = new HashMap<Long, Link>();
			Map<Long, Integer> distMap = new HashMap<Long, Integer>();
			for (Long j : switches){
				if (i.equals(j)){
					distMap.put(j, 0);
				} else {
					distMap.put(j, 10000); // assume we won't have more than 10000 switches
				}
			}
			bestDists.put(i, distMap);
			bestRoutes.put(i, linkMap);
		}
		for (Link link : this.getLinks()){
			Long src = link.getSrc();
			Long dst = link.getDst();
			bestDists.get(src).put(dst, 1);
			bestDists.get(dst).put(src, 1);
			bestRoutes.get(src).put(dst, link);
			bestRoutes.get(dst).put(src, link);
		}

		for (Long k : switches){
			for (Long i : switches){
				for (Long j : switches){
					if (bestDists.get(i).get(j) > bestDists.get(i).get(k) + bestDists.get(k).get(j))
					{
						bestDists.get(i).put(j, bestDists.get(i).get(k) + bestDists.get(k).get(j));
						bestRoutes.get(i).put(j, bestRoutes.get(i).get(k));
					}
				}
			}
		}
		printBestRoutes(bestRoutes);
		return bestRoutes;
	}

	private void printBestRoutes(Map<Long, Map<Long, Link>> bestRoutes){
		System.out.println("-----------------------------Path table start-----------------------------------");
	    for (Long src : bestRoutes.keySet()){
	        for (Long dst : bestRoutes.get(src).keySet()){
	            System.out.println(String.format("s%s -> s%s : %s", src, dst, bestRoutes.get(src).get(dst)));
            }
        }
		System.out.println("-----------------------------Path table end-------------------------------------");
    }

	/**
	 * Event handler called when link goes up or down.
	 * @param update information about the change in link state
	 */
	@Override
	public void linkDiscoveryUpdate(LDUpdate update) 
	{ this.linkDiscoveryUpdate(Arrays.asList(update)); }
	
	/**
     * Event handler called when the IP address of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceIPV4AddrChanged(IDevice device) 
	{ this.deviceAdded(device); }

	/**
     * Event handler called when the VLAN of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceVlanChanged(IDevice device) 
	{ /* Nothing we need to do, since we're not using VLANs */ }
	
	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId) 
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }

	/**
	 * Event handler called when a port on a switch goes up or down, or is
	 * added or removed.
	 * @param DPID for the switch
	 * @param port the port on the switch whose status changed
	 * @param type the type of status change (up, down, add, remove)
	 */
	@Override
	public void switchPortChanged(long switchId, ImmutablePort port,
			PortChangeType type) 
	{ /* Nothing we need to do, since we'll get a linkDiscoveryUpdate event */ }

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return this.MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) 
	{ return false; }

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) 
	{ return false; }
	
    /**
     * Tell the module system which services we provide.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() 
	{ return null; }

	/**
     * Tell the module system which services we implement.
     */
	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> 
			getServiceImpls() 
	{ return null; }

	/**
     * Tell the module system which modules we depend on.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> 
			getModuleDependencies() 
	{
		Collection<Class<? extends IFloodlightService >> floodlightService =
	            new ArrayList<Class<? extends IFloodlightService>>();
        floodlightService.add(IFloodlightProviderService.class);
        floodlightService.add(ILinkDiscoveryService.class);
        floodlightService.add(IDeviceService.class);
        return floodlightService;
	}
}
