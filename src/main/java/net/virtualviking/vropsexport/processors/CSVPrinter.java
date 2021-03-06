/* 
 * Copyright 2017 Pontus Rydin
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package net.virtualviking.vropsexport.processors;

import net.virtualviking.vropsexport.Config;
import net.virtualviking.vropsexport.DataProvider;
import net.virtualviking.vropsexport.ExporterException;
import net.virtualviking.vropsexport.ProgressMonitor;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import net.virtualviking.vropsexport.RowMetadata;

import org.apache.http.HttpException;

import java.util.Date;
import java.util.Iterator;

import net.virtualviking.vropsexport.Row;
import java.io.BufferedWriter;

import net.virtualviking.vropsexport.Rowset;
import net.virtualviking.vropsexport.RowsetProcessor;
import net.virtualviking.vropsexport.RowsetProcessorFacotry;

public class CSVPrinter implements RowsetProcessor {

	public static class Factory implements RowsetProcessorFacotry {
		@Override
		public RowsetProcessor makeFromConfig(BufferedWriter bw, Config config, DataProvider dp, ProgressMonitor pm) {
			return new CSVPrinter(bw, new SimpleDateFormat(config.getDateFormat()), dp, pm);
		}
	}

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
	public void preamble(RowMetadata meta, Config conf) throws ExporterException {
		try {
			// Output table header
			//
			bw.write("timestamp,resName");
			for (Config.Field fld : conf.getFields()) {
				bw.write(",");
				bw.write(fld.getAlias());
			}
			bw.newLine();
		} catch(IOException e) {
			throw new ExporterException(e);
		}
	}

	@Override
	public void process(Rowset rowset, RowMetadata meta) throws ExporterException {
		try {
			synchronized (bw) {
				for (Row row : rowset.getRows().values()) {
					long t = row.getTimestamp();
					if (df != null) {
						bw.write("\"" + df.format(new Date(t)) + "\"");
					} else
						bw.write("\"" + t + "\"");
					bw.write(",\"");
					bw.write(dp.getResourceName(rowset.getResourceId()));
					bw.write("\"");
					Iterator<Object> itor = row.iterator(meta);
					while (itor.hasNext()) {
						Object o = itor.next();
						bw.write(",\"");
						bw.write(o != null ? o.toString() : "");
						bw.write('"');
					}
					bw.newLine();
					bw.flush();
				}
			}
			if (pm != null)
				pm.reportProgress(1);
		} catch (IOException | HttpException e) {
			throw new ExporterException(e);
		}
	}
}
