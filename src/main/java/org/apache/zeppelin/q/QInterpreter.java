package org.apache.zeppelin.q;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kx.c;
import kx.c.Dict;
import kx.c.Flip;
import kx.c.KException;

public class QInterpreter extends Interpreter {

	private static final Logger LOGGER = LoggerFactory.getLogger(QInterpreter.class);

	private static final char NEWLINE = '\n';
	private static final char TAB = '\t';

	static final String HOST_PROPERTY = "q.server.host";
	static final String PORT_PROPERTY = "q.server.port";
	static final String USER_PROPERTY = "q.server.user";
	static final String PASSWORD_PROPERTY = "q.server.password";
	private static final String MAXLINE_PROPERTY = "q.common.max_count";

	private static final int MAX_LINE_DEFAULT = 1000;

	private int maxLineResults;


	public QInterpreter(Properties property) {
		super(property);
		
		maxLineResults = MAX_LINE_DEFAULT;
	}

	@Override
	public void open() {
		LOGGER.info("Max line: {}", getProperty(MAXLINE_PROPERTY));

		if (properties.containsKey(MAXLINE_PROPERTY)) {
			maxLineResults = Integer.parseInt(getProperty(MAXLINE_PROPERTY));
		}
	}

	@Override
	public void close() {
		LOGGER.info("close");
	}

	@Override
	public void cancel(InterpreterContext context) {
		LOGGER.info("cancel");
	}

	private c getConnection()  {
		String host = getProperty(HOST_PROPERTY);
		int port = Integer.parseInt(getProperty(PORT_PROPERTY));
		String user = getProperty(USER_PROPERTY);
		String password = getProperty(PASSWORD_PROPERTY);

		LOGGER.info("Open connection to:" + host + ", " + port + ", " + user + ":" + password);
		
		try {
			c c = new c(host, port, user + ":" + password);
			return c;
		} catch (KException | IOException e) {
			LOGGER.error("Connection failed.", e);
			return null;
		}
	}

	private void closeConnection(c connection) {
		LOGGER.info("Close connection");
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException ex) {
			}
		}
	}

	@Override
	public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {

		LOGGER.info("Execute q: {}", cmd);

		return executeScript(cmd, contextInterpreter);
	}

	public InterpreterResult executeScript(String cmds, InterpreterContext interpreterContext) {

		c connection = null;
		try {
			connection = getConnection();
			if (connection == null) {
				return new InterpreterResult(Code.ERROR, "Connection failed.");
			}

			// execute cmd
			InterpreterResult result = null;
			for (String cmd : cmds.split("\n")) {
				LOGGER.info("cmd:" + cmd);
				System.out.println("cmd: " + cmd);
				if(cmd.contains(".zeppelin.") ) {
					if(cmd.startsWith(".zeppelin.")) {
						setInContext(cmd, interpreterContext, connection);
					} else {
						getFromContext(cmd, interpreterContext, connection);
						continue;
					}
				}
				// that's a q comment and then will return any result
				if (cmd.startsWith("/")) {
					continue;
				}
				result = getResults(connection.k(cmd));
			}

			if (result == null)
				return new InterpreterResult(Code.ERROR, "Nothing to execute!");
			
			return result;
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
			return new InterpreterResult(Code.ERROR, ex.getMessage());
		} finally {
			closeConnection(connection);
		}
	}

	private void getFromContext(String cmd, InterpreterContext interpreterContext, c connection) throws KException, IOException {
		Matcher matcher = Pattern.compile("(.+):.zeppelin.(.+)").matcher(cmd);
		if (matcher.matches()) { 
			String newvar = matcher.group(1);
			String var = matcher.group(2);
			LOGGER.info("get [" + var + "] in global zeppelin context");
			InterpreterResultMessage result = (InterpreterResultMessage) interpreterContext.getResourcePool().get(var).get();
			LOGGER.info( String.valueOf( result.getData() ));
			
			for (String row : result.getData().split(String.valueOf(NEWLINE))) {
				String[] split = row.split(String.valueOf(TAB));
			}
//			c.Flip flip = new c.Flip(new c.Dict(new String[] { "time", "sym", "price", "volume" },
//					new Object[] { new c.Timespan[] { new c.Timespan(), new c.Timespan() },
//							new String[] { "ABC", "DEF" }, new double[] { 123.456, 789.012 },
//							new long[] { 100, 200 } }));
//			connection.k("{x}",flip);
			
			int[] col1 = { 1, 2, 3 };
			boolean[] col2 = { true, false, true };
			String[] col3 = { "sym1", "sym2", "sym3" };
			
			String[] colNames = { "col1", "col2", "col3" };
			
			Object[] cols = new Object[3];
			cols[0] = col1;
			cols[1] = col2;
			cols[2] = col3;
			
			c.Flip table = new c.Flip(new c.Dict(colNames, cols));
			
			Object k = connection.k("func",table);
			System.out.println(k);
			Object k2 = connection.k("func:{show x}");
			System.out.println(k2);
		}
	}

	private void setInContext(String cmd, InterpreterContext interpreterContext, c connection) throws KException, IOException {
		Matcher matcher = Pattern.compile("^.zeppelin.(.+):(.+)").matcher(cmd);
		if (matcher.matches()) {
			String var = matcher.group(1);
			String c = matcher.group(2);
			LOGGER.info("put [" + var + ", " + c + "] in global zeppelin context");
			List<InterpreterResultMessage> message = getResults(connection.k(c)).message();
			interpreterContext.getResourcePool().put(var, message.get(0));
			LOGGER.info("check in context" + interpreterContext.getResourcePool().get(var).get());
		}
	}
	
	public static void main(String... args) {
		
//		String[][] a = new String[][] {{"z", "a", "as"}, {"z", "a", "as"}};
//		String[][] b = new String[][] {{"s"}, {"x"}};
//		
//		
//		String[][] both = Stream.of(a, b).flatMap(Stream::of)
//		        .toArray(String[][]::new);
//		
//		Object[][] appendArray2dVar = appendArray2dVar(a, b);
//		System.out.println(both);
		String cmd = "c:.zeppelin.sd.test";
//		 Matcher matcher = Pattern.compile("^.zeppelin.(.+):(.+)").matcher(cmd);
		Matcher matcher = Pattern.compile("(.+).zeppelin.(.+)").matcher(cmd);
		 if(matcher.matches()) {
			 System.out.println(matcher.group(1));
			 System.out.println(matcher.group(2));
		 }
	}

	private InterpreterResult getResults(Object k) {
		if (k instanceof kx.c.KException) {
			kx.c.KException e = (kx.c.KException) k;
			return new InterpreterResult(Code.ERROR, e.getMessage());
		}
		if (k == null) {
			return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.TEXT, "Query executed successfully.");
		}
		
		MutableBoolean maxLimit = new MutableBoolean(false);
		if (k instanceof Object[]) {
			Object[] a = (Object[]) k;
			String msg = serializeArray(a, maxLimit);

			return new InterpreterResult(Code.SUCCESS, msg);
		}
		if (k instanceof Dict) {
			Dict dict = (Dict) k;
			String serializeFlip = serializeDict(dict, maxLimit );
			if (maxLimit.isTrue()) {
				return new InterpreterResult(Code.SUCCESS,
						Arrays.asList(new InterpreterResultMessage(InterpreterResult.Type.TEXT, "Shows only " + maxLineResults + " rows"),
								new InterpreterResultMessage(InterpreterResult.Type.TABLE, serializeFlip)));
			}
			return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.TABLE, serializeFlip);
		}
		if (k instanceof Flip) {
			Flip flip = (Flip) k;
			String serializeFlip = serializeFlip(flip, maxLimit);
			if (maxLimit.isTrue()) {
				return new InterpreterResult(Code.SUCCESS,
						Arrays.asList(new InterpreterResultMessage(InterpreterResult.Type.TEXT, "Shows only " + maxLineResults + " rows"),
								new InterpreterResultMessage(InterpreterResult.Type.TABLE, serializeFlip)));
			}
			return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.TABLE, serializeFlip);
		}
		else {
			return new InterpreterResult(Code.SUCCESS, String.valueOf(k));
		}
	}

	

	/**
	 * Serialize Dict dataset
	 * @param dict
	 * @param maxLimit
	 * @return
	 */
	protected String serializeDict(Dict dict, MutableBoolean maxLimit) {
		if (dict.x instanceof Flip && dict.y instanceof Flip) {
			Flip mergedFlip = mergeFlips((Flip)dict.x, (Flip)dict.y); 
			return serializeFlip(mergedFlip, maxLimit );
		}
		
		StringBuilder msg = new StringBuilder();
		
		int displayRowCount = 0;
		for (int i = 0; i < Array.getLength(dict.x); i++) {
			if (i > 0) {
				msg.append(NEWLINE);
				displayRowCount++;
			}
			
			if (displayRowCount >= maxLineResults) {
				maxLimit.setValue(true);
				return msg.toString();
			}
			
			msg.append(c.at(dict.x, i)).append(TAB).append(c.at(dict.y, i));
		}
		return msg.toString();
	}
	
	
	/**
	 * Serialize Flip dataset
	 * @param flip
	 * @param maxLimit
	 * @return
	 */
	protected String serializeFlip(Flip flip, MutableBoolean maxLimit) {
		StringBuilder msg = new StringBuilder();

		int columnCount = getColumnCount(flip);
		for (int col = 0; col < columnCount; col++) {
			msg.append(flip.x[col]);
			if (col != columnCount) {
				msg.append(TAB);
			}
		}
		msg.append(NEWLINE);

		int displayRowCount = 0;
		for (int row = 0; row < getRowCount(flip); row++) {
			// If we hit the max row number, then interupt
			if (displayRowCount >= maxLineResults) {
				maxLimit.setValue(true);
				return msg.toString();
			}

			for (int col = 0; col < columnCount; col++) {
				Object value = getValueAt(flip, row, col);
				msg.append(convertString(value));
				if (col != columnCount) {
					msg.append(TAB);
				}
			}
			msg.append(NEWLINE);
			displayRowCount++;
		}
		return msg.toString();
	}
	
	/**
	 * Serialize array
	 * @param a
	 * @return
	 */
	private String serializeArray(Object[] a,  MutableBoolean maxLimit) {
		StringBuilder msg = new StringBuilder();
		
		int displayRowCount = 0;
		for (int i = 0; i < Array.getLength(a); i++) {
			if (i > 0) {
				msg.append(NEWLINE);
				displayRowCount++;
			}
			if (displayRowCount >= maxLineResults) {
				maxLimit.setValue(true);
				return msg.toString();
			}
			
			msg.append(c.at(a, i));
		}
		return msg.toString();
	}

	
	/**
	 * Merge 2 Flips in 1
	 * @param flip1
	 * @param flip2
	 * @return
	 */
	private Flip mergeFlips(Flip flip1, Flip flip2) {
		String[] x = Stream.concat(Arrays.stream(flip1.x), Arrays.stream(flip2.x))
                .toArray(String[]::new);
		
		Object[] y = Stream.concat(Arrays.stream(flip1.y), Arrays.stream(flip2.y))
                .toArray(Object[]::new);
		
		Dict dict = new Dict(x, y);
		return new Flip(dict);
	}

	private String convertString(Object value) {
		String str;
		if (value instanceof char[]) {
			char[] chars = (char[]) value;
			str = String.valueOf(chars);
		} else {
			str = String.valueOf(value);
		}
		return str;
	}

	private int getRowCount(c.Flip flip) {
		return Array.getLength(flip.y[0]);
	}

	private int getColumnCount(c.Flip flip) {
		return flip.y.length;
	}

	private Object getValueAt(c.Flip flip, int rowIndex, int columnIndex) {
		return c.at(flip.y[columnIndex], rowIndex);
	}

	@Override
	public FormType getFormType() {
		return FormType.SIMPLE;
	}

	@Override
	public int getProgress(InterpreterContext arg0) {
		return 0;
	}

	@Override
	public Scheduler getScheduler() {
		return SchedulerFactory.singleton().createOrGetParallelScheduler(QInterpreter.class.getName() + this.hashCode(),
				10);
	}
}
