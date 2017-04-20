package net.virtualviking.vropsexport;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;

import org.apache.http.HttpException;
import org.junit.Test;

import net.virtualviking.vropsexport.Config;
import net.virtualviking.vropsexport.ConfigLoader;
import net.virtualviking.vropsexport.ExporterException;
import net.virtualviking.vropsexport.StatsProcessor;
import net.virtualviking.vropsexport.processors.CSVPrinter;

public class TestStatsProcessor {
	@Test
	public void testProcess() throws IOException, ExporterException, HttpException  {
		Config conf = ConfigLoader.parse(new FileReader("testdata/vmfields.yaml"));
		StatsProcessor sp = new StatsProcessor(conf, null, new LRUCache<>(10));
		RowsetProcessor rp = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(System.out)), new SimpleDateFormat("yyy-MM-dd HH:mm"), null, null);
		sp.process(new FileInputStream("testdata/stats.json"), rp, System.currentTimeMillis() - 300000, System.currentTimeMillis());
	}
}
