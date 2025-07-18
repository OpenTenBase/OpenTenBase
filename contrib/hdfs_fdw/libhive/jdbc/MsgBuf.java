/*-------------------------------------------------------------------------
 *
 * MsgBuf.java
 * 		Wrapper class to return error messages or data from java to C
 *
 * Copyright (c) 2019-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		MsgBuf.java
 *
 *-------------------------------------------------------------------------
 */

import java.lang.StringBuffer;

public class MsgBuf
{
	private StringBuffer message;

	public MsgBuf(String msg)
	{
		message = new StringBuffer(msg);
	}

	public String getVal()
	{
		return message.toString();
	}

	public void resetVal()
	{
		message.delete(0, message.length());
	}

	public void catVal(String msg)
	{
		message.append(msg);
	}

	public void catVal(int val)
	{
		message.append(val);
	}
}

