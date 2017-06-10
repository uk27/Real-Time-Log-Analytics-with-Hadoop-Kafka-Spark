package hadoopTest.hadoopTest;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

public class MyTailerListener implements TailerListener {

	@Override
	public void fileNotFound() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fileRotated() {
		// TODO Auto-generated method stub

	}

	@Override
	public void handle(String line) {
		// TODO Auto-generated method stub
		System.out.println(line);
		System.out.println("yeh bhi karo");
	}

	@Override
	public void handle(Exception arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(Tailer arg0) {
		// TODO Auto-generated method stub

	}

}
