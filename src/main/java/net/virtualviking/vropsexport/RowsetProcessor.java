package net.virtualviking.vropsexport;

public interface RowsetProcessor {
	public void process(Rowset rowset, RowMetadata meta) throws ExporterException;
}
