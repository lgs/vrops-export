package net.virtualviking.vropsexport.processors;

import java.util.List;

import net.virtualviking.vropsexport.ExporterException;
import net.virtualviking.vropsexport.LRUCache;
import net.virtualviking.vropsexport.Row;
import net.virtualviking.vropsexport.RowMetadata;
import net.virtualviking.vropsexport.Rowset;
import net.virtualviking.vropsexport.RowsetProcessor;

public class ParentSplicer implements RowsetProcessor {
	
	private final Rowset childRowset; 
	
	private final LRUCache<String, Rowset> rowsetCache;

	public ParentSplicer(Rowset childRowset, LRUCache<String, Rowset> rowsetCache) {
		super();
		this.childRowset = childRowset;
		this.rowsetCache = rowsetCache;
	}

	@Override
	public void process(Rowset rowset, RowMetadata meta) throws ExporterException {
		spliceRows(childRowset, rowset);
		synchronized(this.rowsetCache) {
			this.rowsetCache.put(rowset.getResourceId(), rowset);
		}
	}
	
	public static void spliceRows(Rowset child, Rowset parent) {
		for(Row pRow : parent.getRows().values()) {
			Row cRow = child.getRows().get(pRow.getTimestamp());
			if(cRow != null) {
				for(int j = 0; j < pRow.getNumMetrics(); ++j) {
					Double d = pRow.getMetric(j);
					if(d != null)
						cRow.setMetric(j, d);
				}
				for(int j = 0; j < pRow.getNumProps(); ++j) {
					String s = pRow.getProp(j);
					if(s != null)
						cRow.setProp(j, s);
				}
			}
		}
	}
}
