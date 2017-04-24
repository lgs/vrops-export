package net.virtualviking.vropsexport;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

public class SelfDeletingFileInputStream extends InputStream {
	
	private FileInputStream backer;
	
	private File file;
	
	public SelfDeletingFileInputStream(String filename) throws FileNotFoundException {
		this.file = new File(filename);
		this.backer = new FileInputStream(this.file);
	}
	
	public SelfDeletingFileInputStream(File file) throws FileNotFoundException {
		this.file = file;
		this.backer = new FileInputStream(this.file);
	}

	@Override
	public int read() throws IOException {
		return backer.read();
	}

	public int hashCode() {
		return backer.hashCode();
	}

	public boolean equals(Object obj) {
		return backer.equals(obj);
	}

	public int read(byte[] b) throws IOException {
		return backer.read(b);
	}

	public int read(byte[] b, int off, int len) throws IOException {
		return backer.read(b, off, len);
	}

	public String toString() {
		return backer.toString();
	}

	public long skip(long n) throws IOException {
		return backer.skip(n);
	}

	public int available() throws IOException {
		return backer.available();
	}

	public void mark(int readlimit) {
		backer.mark(readlimit);
	}

	public void close() throws IOException {
		backer.close();
		file.delete();
	}

	public final FileDescriptor getFD() throws IOException {
		return backer.getFD();
	}

	public FileChannel getChannel() {
		return backer.getChannel();
	}

	public void reset() throws IOException {
		backer.reset();
	}

	public boolean markSupported() {
		return backer.markSupported();
	}
}
