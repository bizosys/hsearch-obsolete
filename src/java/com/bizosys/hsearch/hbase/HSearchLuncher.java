/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.hbase;


import java.awt.AWTException;
import java.awt.Image;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.HQuorumPeer;

/**
 * Launches All servers related to HSearch in a standalone mode.
 * Currently it launches HBase server.
 * TODO:// Allows Tomcat to be launched from here too
 * @author karan
 *
 */
public class HSearchLuncher {

	static TrayIcon trayIcon;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		if ( ! SystemTray.isSupported() ) throw new Exception ("Tray Icon is not supported.");
		
	    SystemTray tray = SystemTray.getSystemTray();
	    Image image = Toolkit.getDefaultToolkit().getImage("favicon.gif");

	    ActionListener exitListener = new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	new OHBase("stop", trayIcon).start();
	        	try {
	        		Thread.sleep(1000);
	        	} catch (Exception ex) {
	        	}
	        	new OZookeeper("stop", trayIcon).start();
	            System.exit(0);
	        }
	    };
		    
	    ActionListener startListener = new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	new OHBase("start", trayIcon).start();
	        	try {Thread.sleep(3000);} catch (Exception ex) {}
	        	System.out.println("HBase Server started");
	        }
	    };	
	    
	    ActionListener stopListener = new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	new OHBase("stop", trayIcon).start();
	        	try {
	        		Thread.sleep(1000);
	        	} catch (Exception ex) {
	        	}
	        	//new OZookeeper("stop", trayIcon).start();
	        }
	    };		    
		    
	    PopupMenu popup = new PopupMenu();
		            
	    MenuItem startItem = new MenuItem("Start HBase");
	    startItem.addActionListener(startListener);
	    popup.add(startItem);
		    
	    MenuItem stopItem = new MenuItem("Stop HBase");
	    stopItem.addActionListener(stopListener);
	    popup.add(stopItem);
		    
		    
	    MenuItem exitItem = new MenuItem("Exit");
	    exitItem.addActionListener(exitListener);
	    popup.add(exitItem);

	    trayIcon = new TrayIcon(image, "Hsearch", popup);
	    trayIcon.setImageAutoSize(true);

	    try {
	        tray.add(trayIcon);
	    } catch (AWTException e) {
	        System.err.println("TrayIcon could not be added.");
	    }
	}
	
	/**
	 * Starting the zookeeper
	 * @author karan
	 *
	 */
	static class OZookeeper extends Thread {
	    String command = "start";
	    TrayIcon trayIcon = null;
	    
		public OZookeeper(String command, TrayIcon trayIcon) {
	    	this.command = command;
	    	this.trayIcon = trayIcon;
	    }
		
	    public void run() {
	    	try {
	    		HQuorumPeer.main(new String[]{this.command});
	    	} catch (Exception ex) {
	            trayIcon.displayMessage("HSearch Server", 
		                "Error during Server " + this.command + "\n" + ex.getMessage(),
                    TrayIcon.MessageType.ERROR);
	    	}
	    }
	}
	
	/**
	 * Starting hbase
	 * @author karan
	 *
	 */
	static class OHBase extends Thread {
	    String command = "start";
	    TrayIcon trayIcon = null;

	    public OHBase(String command, TrayIcon trayIcon) {
	    	this.command = command;
	    	this.trayIcon = trayIcon;
	    }
	    public void run() {
	    	try {
	    		HMaster.main(new String[]{command});
	    	} catch (Exception ex) {
	            trayIcon.displayMessage("HBase Server", 
	                "Error during Server " + this.command + "\n" + ex.getMessage(),
	                TrayIcon.MessageType.ERROR);
	    	}
	    }
	}
}
