package net.virtualviking.vropsexport;

import java.util.List;
import java.util.TreeMap;

public class Rowset {	
	private final String resourceId;
	
	private final TreeMap<Long, Row> rows;

	public Rowset(String resourceId, TreeMap<Long, Row> rows) {
		super();
		this.resourceId = resourceId;
		this.rows = rows;
	}

	public String getResourceId() {
		return resourceId;
	}

	public TreeMap<Long, Row> getRows() {
		return rows;
	}
}
