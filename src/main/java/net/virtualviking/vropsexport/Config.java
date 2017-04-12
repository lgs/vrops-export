package net.virtualviking.vropsexport;

public class Config {
	public static class Field {
		private String alias;
		private String metric;
		private String prop;
		
		public Field() {
		}
	
		public Field(String alias, String name, boolean isMetric) {
			super();
			this.alias = alias;
			if(isMetric)
				this.metric = name;
			else 
				this.prop = name;
		}

		public String getAlias() {
			return alias;
		}

		public void setAlias(String alias) {
			this.alias = alias;
		}

		public String getMetric() {
			return metric;
		}
		
		public boolean hasMetric() {
			return metric != null;
		}

		public void setMetric(String metric) {
			this.metric = metric;
		}

		public String getProp() {
			return prop;
		}

		public void setProp(String prop) {
			this.prop = prop;
		}
		
		public boolean hasProp() {
			return this.prop != null;
		}
	}
	private Field[] fields;
	private String resourceType;
	private String rollupType;
	private long rollupMinutes;
	private String dateFormat;
	
	public Config() {
	}

	public Field[] getFields() {
		return fields;
	}

	public void setFields(Field[] fields) {
		this.fields = fields;
	}

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public String getRollupType() {
		return rollupType;
	}

	public void setRollupType(String rollupType) {
		this.rollupType = rollupType;
	}

	public long getRollupMinutes() {
		return rollupMinutes;
	}

	public void setRollupMinutes(long rollup) {
		this.rollupMinutes = rollup;
	}
	
	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public boolean hasProps() {
		for(Field f : fields) {
			if(f.hasProp())
				return true;
		}
		return false;
	}
	
	public boolean hasMetrics() {
		for(Field f : fields) {
			if(f.hasMetric())
				return true;
		}
		return false;
	}
}
