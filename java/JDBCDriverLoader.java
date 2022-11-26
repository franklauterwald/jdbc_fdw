/*-------------------------------------------------------------------------
 *
 *		  foreign-data wrapper for JDBC
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Atri Sharma <atri.jiit@gmail.com>
 *
 * IDENTIFICATION
 *		  jdbc_fdw/JDBCDriverLoader.java
 *
 *-------------------------------------------------------------------------
 */

import java.net.URL;
import java.net.URLClassLoader;
import java.net.MalformedURLException;
 
public class JDBCDriverLoader extends URLClassLoader {

	public JDBCDriverLoader(URL[] path)
	{
		super(path);
	}

	public void	addPath(String path) throws MalformedURLException {
		addURL(new URL (path));
	}

	/*
	 * Checks if a class of given classname has been loaded by the loader or not.
	 * Only required because stupid URLClassLoader has findLoadedClass as protected final
	 */
	public boolean isClassLoaded(String className) { return (findLoadedClass(className) != null);	}
}
