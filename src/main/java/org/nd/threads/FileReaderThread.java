package org.nd.threads;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.nd.dto.FileIdAndContent;

public class FileReaderThread implements Callable<FileIdAndContent> {
	
	private String id;
	private String path;
		
	public FileReaderThread() {
		super();
	}

	public FileReaderThread(String id, String path) {
		super();
		this.id = id;
		this.path = path;
	}



	@Override
	public FileIdAndContent call() throws Exception {
		try {
			String content = FileUtils.readFileToString(new File(path) , StandardCharsets.UTF_8);
			FileIdAndContent file = new FileIdAndContent(id, content);
			return file;
		} catch (Exception e) {
			FileIdAndContent file = new FileIdAndContent(id, "");
			return file;
		}

	}

}
