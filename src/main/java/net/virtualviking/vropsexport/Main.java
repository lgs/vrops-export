package net.virtualviking.vropsexport;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.net.ssl.SSLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Main {
    public static void main(String[] args) throws Exception {
    	for(String s : args) 
    		System.out.println(s);
    	// Parse command line
    	//
    	CommandLineParser parser = new DefaultParser();
    	Options opts = defineOptions();
    	CommandLine commandLine = null;
    	try {
    		commandLine = parser.parse(opts, args);
    	} catch(ParseException e) {
    		System.err.println("Error parsing command. Use -h option for help. Details: " + e.getMessage());
    		System.exit(1);
    	}
    	// Help requested?
    	//
    	if(commandLine.hasOption('h')) {
    		HelpFormatter hf = new HelpFormatter();
    		String head = "Exports vRealize Operations Metrics";
    		String foot = "Project home: https://github.com/prydin/vrops-export";
    		hf.printHelp("exporttool", head, opts, foot, true);
    		System.exit(0);
    	}
    	try {
    		// Extract command options and do sanity checks.
    		//
    		String defFile = commandLine.getOptionValue('d');
    		if(defFile == null) 
    			throw new ExporterException("Definition file must be specified");
    		String username = commandLine.getOptionValue('u');
    		if(username == null)
    			throw new ExporterException("Username must be specified");
    		String password = commandLine.getOptionValue('p');
    		if(password == null)
    			throw new ExporterException("Password must be specified");
    		String host = commandLine.getOptionValue('H');
    		if(host == null)
    			throw new ExporterException("Host URL must be specified");
    		String output = commandLine.getOptionValue('o');
    		
    		// Deal with lookback/time period
    		//
    		String lb = commandLine.getOptionValue('l');
    		String startS = commandLine.getOptionValue('s');
    		String endS = commandLine.getOptionValue('e');
    		if(lb != null && (endS != null || startS != null)) 
    			throw new ExporterException("Lookback and start/end can't be specified at the same time");
    		if(startS != null ^ endS != null)	    			
    			throw new ExporterException("Both start and end must be specified");
    		boolean trustCerts = commandLine.hasOption('i');
    		String namePattern = commandLine.getOptionValue('n');
    		String parentSpec = commandLine.getOptionValue('P');
    		if(namePattern != null && parentSpec != null) 
    			throw new ExporterException("Name filter is not supported with parent is specified");
    		boolean quiet = commandLine.hasOption('q');
    		
    		// Output to stdout implies quiet mode
    		//
    		if(output == null)
    			quiet = true;
    		
    		// Read definition and run it!
    		//
	    	FileReader fr = new FileReader(defFile);
	    	try {
		    	Config conf = ConfigLoader.parse(fr);
		    	
		    	// Deal with start and end dates
		    	//
		        long end = System.currentTimeMillis();
	        	long lbMs = lb != null ? parseLookback(lb) : 1000L * 60L * 60L * 24L;
	        	long begin = end - lbMs;
	        	if(startS != null) {
	        		if(conf.getDateFormat() == null)
	        			throw new ExporterException("Date format must be specified in config file if -e and -s are used");
	        		DateFormat df = new SimpleDateFormat(conf.getDateFormat());
	        		try {
	        			end = df.parse(endS).getTime();
	        			begin = df.parse(startS).getTime();
	        		} catch(java.text.ParseException e) {
	        			throw new ExporterException(e.getMessage());
	        		}
	        	}
		        Exporter exporter = new Exporter(host, username, password, trustCerts, conf);
		        Writer wrt = output != null ? new FileWriter(output) : new OutputStreamWriter(System.out);
		        exporter.exportTo(wrt, begin, end, namePattern, parentSpec, quiet);
	    	} finally {
	    		fr.close();
	    	}
    	} catch(ExporterException e) {
    		System.err.println("ERROR: " + e.getMessage());
    		System.exit(1);
    	}
    	catch(SSLException e) {
    		System.err.println("SSL ERROR: " + e.getMessage() + "\n\nConsider using the -i option!");
    		System.exit(1);
    	}
    }
    
    private static Options defineOptions() {
		Options opts = new Options();
		opts.addOption("d", "definition", true, "Path to definition file");
		opts.addOption("l", "lookback", true, "Lookback time");
		opts.addOption("s", "start", true, "Time period start (date format in definition file)");
		opts.addOption("e", "end", true, "Time period end (date format in definition file)");
		opts.addOption("n", "namequery", true, "Name query");
		opts.addOption("P", "parent", true, "Parent resource (ResourceKind:resourceName)");
		opts.addOption("u", "username", true, "Username");
		opts.addOption("p", "password", true, "Password");
		opts.addOption("o", "output", true, "Output file");
		opts.addOption("H", "host", true, "URL to vRealize Operations Host");
		opts.addOption("q", "quiet", false, "Quiet mode (no progress counter)");
		opts.addOption("i", "ignore-cert", false, "Trust any cert");
		opts.addOption("h", "help", false, "Print a short help");
		return opts;
	}		
    
    private static long parseLookback(String lb) throws ExporterException {
    	long scale = 1;
    	char unit = lb.charAt(lb.length() - 1);
    	switch(unit) {
    	case 'd':
    		scale *= 24;
    	case 'h':
    		scale *= 60;
    	case 'm':
    		scale *= 60;
    	case 's':
    		scale *= 1000;
    		break;
    	default:
    		throw new ExporterException("Cannot parse time unit");
    	}
    	try {
    		long t = Long.parseLong(lb.substring(0, lb.length() - 1));
    		return t * scale;
    	} catch(NumberFormatException e) {
    		throw new ExporterException("Cannot parse time value");
    	}
    }
}
