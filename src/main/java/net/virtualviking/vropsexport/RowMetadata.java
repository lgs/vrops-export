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
package net.virtualviking.vropsexport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RowMetadata {
	private final String resourceType;
	
	private final Map<String, Integer> metricMap = new HashMap<>();
	
	private final Map<String, Integer> propMap = new HashMap<>();
	
	private int[] propInsertionPoints;
	
	private Pattern parentPattern = Pattern.compile("^\\$parent\\:([_A-Za-z][_A-Za-z0-9]*)\\.(.+)$");
	
	public RowMetadata(Config conf) {
		this.resourceType = conf.getResourceType();
		int mp = 0;
		int pp = 0;
		List<Integer> pip = new ArrayList<>(); 
		for(Config.Field fld : conf.getFields()) {
			if(fld.hasMetric()) 
				metricMap.put(fld.getMetric(), mp++);
			if(fld.hasProp()) {
				propMap.put(fld.getProp(), pp++);
				pip.add(mp);
			}
		}
		propInsertionPoints = new int[pip.size()];
		for(int i = 0; i < pip.size(); ++i) 
			propInsertionPoints[i] = pip.get(i);
	}
	
	private RowMetadata(RowMetadata child) throws ExporterException {
		this.propInsertionPoints = child.propInsertionPoints;
		String t = null;
		for(Map.Entry<String, Integer> e : child.propMap.entrySet()) {
			String p = e.getKey();
			Matcher m = parentPattern.matcher(p);
			if(m.matches())  {
				if(t == null)
					t = m.group(1);
				else if(!t.equals(m.group(1))) {
					throw new ExporterException("Only one parent type is currently supported");
				}
				propMap.put(m.group(2), e.getValue());
			} else {
				propMap.put("_placeholder_" + p, e.getValue());
			}
		}
		for(Map.Entry<String, Integer> e : child.metricMap.entrySet()) {
			String mt = e.getKey();
			Matcher m = parentPattern.matcher(mt);
			if(m.matches()) {
				if(t == null)
					t = m.group(1);
				else if(!t.equals(m.group(1))) {
					throw new ExporterException("Only one parent type is currently supported");
				}
				metricMap.put(m.group(2), e.getValue());
			} else {
				metricMap.put("_placholder_" + mt, e.getValue());
			}
		}
		this.resourceType = t;
	}
	
	public RowMetadata forParent() throws ExporterException {
		return new RowMetadata(this);
	}

	public Map<String, Integer> getMetricMap() {
		return metricMap;
	}

	public Map<String, Integer> getPropMap() {
		return propMap;
	}

	public int[] getPropInsertionPoints() {
		return propInsertionPoints;
	}
	
	public int getMetricIndex(String metric) {
		return metricMap.containsKey(metric) ? metricMap.get(metric) : -1;
	}
	
	public int getPropertyIndex(String property) {
		return propMap.containsKey(property) ? propMap.get(property) : -1;
	}
	
	public Row newRow(long timestamp) {
		return new Row(timestamp, metricMap.size(), propMap.size());
	}
	
	public String getResourceType() {
		return resourceType;
	}
	
	public boolean isValid() {
		return resourceType != null;
	}
}
