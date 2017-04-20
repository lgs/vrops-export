package net.virtualviking.vropsexport;

import java.util.BitSet;
import java.util.NoSuchElementException;

public class Row {
	private final long timestamp;
	
	private final BitSet definedMetrics;
		
	private final double[] metrics;
		
	private final String[] props;

	public Row(long timestamp, int nMetrics, int nProps) {
		super();
		this.timestamp = timestamp;
		this.metrics = new double[nMetrics];
		this.props = new String[nProps];
		this.definedMetrics = new BitSet(nMetrics);
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Double getMetric(int i) {
		return definedMetrics.get(i) ? metrics[i] : null;
	}
	
	public String getProp(int i) {
		return props[i];
	}
	
	public void setMetric(int i, double m) {
		metrics[i] = m;
		definedMetrics.set(i);
	}
	
	public void setProp(int i, String prop) {
		props[i] = prop;
	}
	
	public java.util.Iterator<Object> iterator(RowMetadata meta) {
		return new Iterator(meta);
	}
	
	public int getNumProps() {
		return props.length;
	}
	
	public int getNumMetrics() {
		return metrics.length;
	}
	
	private class Iterator implements java.util.Iterator<Object> {
		private int mc;
		
		private int pc;
				
		private RowMetadata meta;
		
		public Iterator(RowMetadata meta) {
			this.meta = meta;
		}

		@Override
		public boolean hasNext() {
			return pc < props.length || mc < metrics.length;
		}

		@Override
		public Object next() {
			if(!this.hasNext()) 
				throw new NoSuchElementException();
			if(meta.getPropInsertionPoints()[pc] == mc)
				return getProp(pc++);
			return getMetric(mc++);
		}
	}
}
