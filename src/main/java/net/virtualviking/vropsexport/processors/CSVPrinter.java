package net.virtualviking.vropsexport.processors;

import net.virtualviking.vropsexport.DataProvider;
import net.virtualviking.vropsexport.ExporterException;
import net.virtualviking.vropsexport.ProgressMonitor;

import java.io.IOException;
import java.text.DateFormat;

import net.virtualviking.vropsexport.RowMetadata;

import org.apache.http.HttpException;

import java.util.Date;
import java.util.Iterator;

import net.virtualviking.vropsexport.Row;
import java.io.BufferedWriter;

import net.virtualviking.vropsexport.Rowset;
import net.virtualviking.vropsexport.RowsetProcessor;

public class CSVPrinter implements RowsetProcessor {
	
	private final BufferedWriter bw;
	
	private final DateFormat df;
	
	private final DataProvider dp;
	
	private final ProgressMonitor pm;
		
	public CSVPrinter(BufferedWriter bw, DateFormat df, DataProvider dp, ProgressMonitor pm) {
		this.bw = bw;
		this.df = df;
		this.dp = dp;
		this.pm = pm;
	}

	@Override
	public void process(Rowset rowset, RowMetadata meta) throws ExporterException {
		try {
			synchronized(bw) {
				for(Row row : rowset.getRows().values()) {
					long t = row.getTimestamp();
					if(df != null) {
						bw.write("\"" + df.format(new Date(t)) + "\"");
					} else
						bw.write("\"" + t + "\"");
					bw.write(",\"");
					bw.write(dp.getResourceName(rowset.getResourceId()));
					bw.write("\"");
					Iterator<Object> itor = row.iterator(meta);
					while(itor.hasNext()) {
						Object o = itor.next();
						bw.write(",\"");
						bw.write(o != null ? o.toString() : "");
						bw.write('"');
					}
					bw.newLine();
					bw.flush();
				}
			}
			if(pm != null)
				pm.reportProgress(1);
		} catch(IOException|HttpException e) {
			throw new ExporterException(e);
		}
	}
}
