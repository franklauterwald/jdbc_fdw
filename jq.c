/*
 * ---------------------------------------------
 * jq.c Implementation of Low level
 * JDBC based functions replacing the libpq-fe functions
 *
 * Heimir Sverrisson, 2015-04-13
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * ---------------------------------------------
 */
#include "postgres.h"
#include "jdbc_fdw.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "commands/defrem.h"
#include "libpq-fe.h"

#include "jni.h"

#define Str(arg) #arg
#define StrValue(arg) Str(arg)
#define STR_INSTALL_DIR StrValue(INSTALL_DIR)
/* Number of days from unix epoch time (1970-01-01) to postgres epoch time (2000-01-01) */
#define POSTGRES_TO_UNIX_EPOCH_DAYS 		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
/* POSTGRES_TO_UNIX_EPOCH_DAYS to microseconds */
#define POSTGRES_TO_UNIX_EPOCH_USECS 		(POSTGRES_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY)

/*
 * Local housekeeping functions and Java objects
 */

static JNIEnv * Jenv;
static JavaVM * jvm;
static JNIMethods * jni_methods;
jobject		java_call;
static bool InterruptFlag;		/* Used for checking for SIGINT interrupt */

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct jdbcFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Structure holding options from the foreign server and user mapping
 * definitions
 */
typedef struct JserverOptions
{
	char	   *url;
	char	   *drivername;
	char	   *username;
	char	   *password;
	int			querytimeout;
	char	   *jarfile;
	int			maxheapsize;
}			JserverOptions;

static JserverOptions opts;

/* Local function prototypes */
static jmethodID find_jni_method_or_fail(jclass JDBCUtilsClass, char *name, char *signature);
static void jq_load_jni_methods(jclass utils_class, JNIMethods *methods);

static int	jdbc_connect_db_complete(Jconn * conn);
static void jdbc_jvm_init(const ForeignServer * server, const UserMapping * user);
static void jdbc_get_server_options(JserverOptions * opts, const ForeignServer * f_server, const UserMapping * f_mapping);
static Jconn * jdbc_create_JDBC_connection(const ForeignServer * server, const UserMapping * user);
/*
 * Uses a String object's content to create an instance of C String
 */
static char *jdbc_convert_string_to_cstring(jobject);
/*
 * Convert byte array to Datum
 */
static Datum jdbc_convert_byte_array_to_datum(jbyteArray);
/*
 * Common function to convert Object value to datum
 */
static Datum jdbc_convert_object_to_datum(Oid, int32, jobject);

/*
 * JVM destroy function
 */
static void jdbc_destroy_jvm();

/*
 * JVM attach function
 */
static void jdbc_attach_jvm();

/*
 * JVM detach function
 */
static void jdbc_detach_jvm();

/*
 * SIGINT interrupt check and process function
 */
static void jdbc_sig_int_interrupt_check_process();

/*
 * clears any exception that is currently being thrown
 */
void		jq_exception_clear(void);

/*
 * check for pending exceptions
 */
void		jq_get_exception(void);

/*
 * get table infomations for importForeignSchema
 */
static List * jq_get_column_infos(Jconn * conn, char *tablename);
static List * jq_get_table_names(Jconn * conn);


static void jq_get_JDBCUtils(Jconn *conn, jclass *JDBCUtilsClass, jobject *JDBCUtilsObject);

/*
 * jdbc_sig_int_interrupt_check_process Checks and processes if SIGINT
 * interrupt occurs
 */
static void
jdbc_sig_int_interrupt_check_process()
{

	if (InterruptFlag == true)
	{
		jq_exception_clear();
		(*Jenv)->CallObjectMethod(Jenv, java_call, jni_methods->cancel);
		jq_get_exception();
		InterruptFlag = false;
		ereport(ERROR, errmsg("Query has been cancelled"));
	}
}

/*
 * jdbc_convert_string_to_cstring Uses a String object passed as a jobject to
 * the function to create an instance of C String.
 */
static char *
jdbc_convert_string_to_cstring(jobject java_cstring)
{
	jclass		JavaString;
	char	   *StringPointer;
	char	   *cString = NULL;

	jdbc_sig_int_interrupt_check_process();

	JavaString = (*Jenv)->FindClass(Jenv, "java/lang/String");
	if (!((*Jenv)->IsInstanceOf(Jenv, java_cstring, JavaString)))
	{
		ereport(ERROR, errmsg("Object not an instance of String class"));
	}

	if (java_cstring != NULL)
	{
		StringPointer = (char *) (*Jenv)->GetStringUTFChars(Jenv,
															(jstring) java_cstring, 0);
		cString = pstrdup(StringPointer);
		(*Jenv)->ReleaseStringUTFChars(Jenv, (jstring) java_cstring, StringPointer);
		(*Jenv)->DeleteLocalRef(Jenv, java_cstring);
	}
	else
	{
		StringPointer = NULL;
	}
	return (cString);
}

/*
 * jdbc_convert_byte_array_to_datum Uses a byte array object passed as a jbyteArray to
 * the function to convert into Datum.
 */
static Datum
jdbc_convert_byte_array_to_datum(jbyteArray byteVal)
{
	Datum		valueDatum;
	jbyte	   *buf = (*Jenv)->GetByteArrayElements(Jenv, byteVal, NULL);
	jsize		size = (*Jenv)->GetArrayLength(Jenv, byteVal);

	jdbc_sig_int_interrupt_check_process();

	if (buf == NULL)
		return 0;

	valueDatum = (Datum) palloc0(size + VARHDRSZ);
	memcpy(VARDATA(valueDatum), buf, size);
	SET_VARSIZE(valueDatum, size + VARHDRSZ);
	return valueDatum;
}

/*
 * jdbc_convert_object_to_datum Convert jobject to Datum value
 */
static Datum
jdbc_convert_object_to_datum(Oid pgtype, int32 pgtypmod, jobject obj)
{
	switch (pgtype)
	{
		case BYTEAOID:
			return jdbc_convert_byte_array_to_datum(obj);
		default:
		{
			/*
			 * By default, data is retrieved as string and then
			 * converted to compatible data types
			 */
			char   *value = jdbc_convert_string_to_cstring(obj);

			if (value != NULL)
				return jdbc_convert_to_pg(pgtype, pgtypmod, value);
			else
				return 0;
		}
	}
}

/*
 * jdbc_destroy_jvm Shuts down the JVM.
 */
static void
jdbc_destroy_jvm()
{
	ereport(DEBUG3, (errmsg("In jdbc_destroy_jvm")));
	(*jvm)->DestroyJavaVM(jvm);
}

/*
 * jdbc_attach_jvm Attach the JVM.
 */
static void
jdbc_attach_jvm()
{
	ereport(DEBUG3, (errmsg("In jdbc_attach_jvm")));
	(*jvm)->AttachCurrentThread(jvm, (void **) &Jenv, NULL);
}

/*
 * jdbc_detach_jvm Detach the JVM.
 */
static void
jdbc_detach_jvm()
{
	ereport(DEBUG3, (errmsg("In jdbc_detach_jvm")));
	(*jvm)->DetachCurrentThread(jvm);
}

/*
 * jdbc_jvm_init Create the JVM which will be used for calling the Java
 * routines that use JDBC to connect and access the foreign database.
 *
 */
void
jdbc_jvm_init(const ForeignServer * server, const UserMapping * user)
{
    // TODO: this is only set upon completion. Multiple simultaneous calls are still racy
	static bool FunctionCallCheck = false;	/* This flag safeguards against
											 * multiple calls of
											 * jdbc_jvm_init() */

	jint		res = -5;		/* Set to a negative value so we can see
								 * whether JVM has been correctly created or
								 * not */
	JavaVMInitArgs vm_args;
	JavaVMOption *options;
	char	   *classpath;
	char	   *maxheapsizeoption = NULL;

	opts.maxheapsize = 0;

	ereport(DEBUG3, (errmsg("In jdbc_jvm_init")));
	jdbc_get_server_options(&opts, server, user);	/* Get the maxheapsize
													 * value (if set) */

	jdbc_sig_int_interrupt_check_process();

	if (FunctionCallCheck == false)
	{
		classpath = (char *) palloc0(strlen(STR_INSTALL_DIR) + 32);
		snprintf(classpath, strlen(STR_INSTALL_DIR) + 32, "-Djava.class.path=%s/jdbc_fdw.jar", STR_INSTALL_DIR);

		if (opts.maxheapsize != 0)
		{						/* If the user has given a value for setting
								 * the max heap size of the JVM */
			options = (JavaVMOption *) palloc0(sizeof(JavaVMOption) * 2);
			maxheapsizeoption = (char *) palloc0(sizeof(int) + 6);
			snprintf(maxheapsizeoption, sizeof(int) + 6, "-Xmx%dm", opts.maxheapsize);
			options[0].optionString = classpath;
			options[1].optionString = maxheapsizeoption;
			vm_args.nOptions = 2;
		}
		else
		{
			options = (JavaVMOption *) palloc0(sizeof(JavaVMOption));
			options[0].optionString = classpath;
			vm_args.nOptions = 1;
		}
		vm_args.version = JNI_VERSION_1_2;
		vm_args.options = options;
		vm_args.ignoreUnrecognized = JNI_FALSE;

		/* Create the Java VM */
		res = JNI_CreateJavaVM(&jvm, (void **) &Jenv, &vm_args);
		if (res < 0)
		{
			ereport(ERROR, (errmsg("Failed to create Java VM") ));
		}
		ereport(DEBUG3, (errmsg("Successfully created a JVM with %d MB heapsize", opts.maxheapsize)));
		InterruptFlag = false;
		/* Register an on_proc_exit handler that shuts down the JVM. */
		on_proc_exit(jdbc_destroy_jvm, 0);
		FunctionCallCheck = true;
	}
	else
	{
		int			JVMEnvStat;

		vm_args.version = JNI_VERSION_1_2;
		JVMEnvStat = (*jvm)->GetEnv(jvm, (void **) &Jenv, vm_args.version);
		if (JVMEnvStat == JNI_EDETACHED)
		{
			ereport(DEBUG3, (errmsg("JVMEnvStat: JNI_EDETACHED; the current thread is not attached to the VM")));
			jdbc_attach_jvm();
		}
		else if (JVMEnvStat == JNI_OK)
		{
			ereport(DEBUG3, (errmsg("JVMEnvStat: JNI_OK")));
		}
		else if (JVMEnvStat == JNI_EVERSION)
		{
			ereport(ERROR, (errmsg("JVMEnvStat: JNI_EVERSION; the specified version is not supported")));
		}
	}

}

/*
 * Create an actual JDBC connection to the foreign server. Precondition:
 * jdbc_jvm_init() has been successfully called. Returns: Jconn.status =
 * CONNECTION_OK and a valid reference to a JDBCUtils class Error return:
 * Jconn.status = CONNECTION_BAD
 */
static Jconn *
jdbc_create_JDBC_connection(const ForeignServer * server, const UserMapping * user)
{
	jstring		stringArray[6];
	jclass		javaString;
	jobjectArray argArray;
	jclass		JDBCUtilsClass;
	jstring		identifierQuoteString;
	char	   *quote_string;
	char	   *querytimeout_string;
	int			numParams = sizeof(stringArray) / sizeof(jstring);	/* Number of parameters
																	 * to Java */
	int			intSize = 10;	/* The string size to allocate for an integer
								 * value */
	int			keyid = server->serverid;	/* key for the hashtable in java
											 * depends on serverid */
	MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);	/* Switch the memory context to TopMemoryContext to avoid the
																		 * case connection is released when execution state finished */
	Jconn	   *conn = (Jconn *) palloc0(sizeof(Jconn));
    jni_methods = (JNIMethods *) palloc0(sizeof(JNIMethods));

	ereport(DEBUG3, (errmsg("In jdbc_create_JDBC_connection")));
	conn->status = CONNECTION_BAD;
	conn->festate = (jdbcFdwExecutionState *) palloc0(sizeof(jdbcFdwExecutionState));
	conn->festate->query = NULL;
	JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
	if (JDBCUtilsClass == NULL)
	{
		ereport(ERROR, errmsg("Failed to find the JDBCUtils class!"));
	}
    jq_load_jni_methods(JDBCUtilsClass, jni_methods);

	/*
	 * Construct the array to pass our parameters Query timeout is an int, we
	 * need a string
	 */
	querytimeout_string = (char *) palloc0(intSize);
	snprintf(querytimeout_string, intSize, "%d", opts.querytimeout);
	stringArray[0] = (*Jenv)->NewStringUTF(Jenv, opts.drivername);
	stringArray[1] = (*Jenv)->NewStringUTF(Jenv, opts.url);
	stringArray[2] = (*Jenv)->NewStringUTF(Jenv, opts.username);
	stringArray[3] = (*Jenv)->NewStringUTF(Jenv, opts.password);
	stringArray[4] = (*Jenv)->NewStringUTF(Jenv, querytimeout_string);
	stringArray[5] = (*Jenv)->NewStringUTF(Jenv, opts.jarfile);
	/* Set up the return value */
	javaString = (*Jenv)->FindClass(Jenv, "java/lang/String");
	argArray = (*Jenv)->NewObjectArray(Jenv, numParams, javaString, stringArray[0]);
	if (argArray == NULL)
	{
		/* Return Java memory */
		for (int i = 0; i < numParams; i++)
		{
			(*Jenv)->DeleteLocalRef(Jenv, stringArray[i]);
		}
		ereport(ERROR, (errmsg("Failed to create argument array")));
	}
	for (int i = 1; i < numParams; i++)
	{
		(*Jenv)->SetObjectArrayElement(Jenv, argArray, i, stringArray[i]);
	}
	conn->JDBCUtilsObject = (*Jenv)->AllocObject(Jenv, JDBCUtilsClass);
	if (conn->JDBCUtilsObject == NULL)
	{
		/* Return Java memory */
		for (int i = 0; i < numParams; i++)
		{
			(*Jenv)->DeleteLocalRef(Jenv, stringArray[i]);
		}
		(*Jenv)->DeleteLocalRef(Jenv, argArray);
		ereport(ERROR, (errmsg("Failed to create jdbc utils object")));
	}
	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->createConnection, keyid, argArray);
	jq_get_exception();
	/* Return Java memory */
	for (int i = 0; i < numParams; i++)
	{
		(*Jenv)->DeleteLocalRef(Jenv, stringArray[i]);
	}
	(*Jenv)->DeleteLocalRef(Jenv, argArray);
	ereport(DEBUG3, errmsg("Created a JDBC connection to : %s", opts.url));
	/* get default identifier quote string */
	jq_exception_clear();
	identifierQuoteString = (jstring) (*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->getIdentifierQuoteString);
	jq_get_exception();
	quote_string = jdbc_convert_string_to_cstring((jobject) identifierQuoteString);
	conn->q_char = pstrdup(quote_string);
	conn->status = CONNECTION_OK;
	pfree(querytimeout_string);
	/* Switch back to old context */
	MemoryContextSwitchTo(oldcontext);
	return conn;
}

/*
 * Fetch the options for a jdbc_fdw foreign server and user mapping.
 */
static void
jdbc_get_server_options(JserverOptions * opts, const ForeignServer * f_server, const UserMapping * f_mapping)
{
	List	   *options;
	ListCell   *lc;

	/* Collect options from server and user mapping */
	options = NIL;
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Loop through the options, and get the values */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "drivername") == 0)
		{
			opts->drivername = defGetString(def);
		}
		if (strcmp(def->defname, "username") == 0)
		{
			opts->username = defGetString(def);
		}
		if (strcmp(def->defname, "querytimeout") == 0)
		{
			opts->querytimeout = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "jarfile") == 0)
		{
			opts->jarfile = defGetString(def);
		}
		if (strcmp(def->defname, "maxheapsize") == 0)
		{
			opts->maxheapsize = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "password") == 0)
		{
			opts->password = defGetString(def);
		}
		if (strcmp(def->defname, "url") == 0)
		{
			opts->url = defGetString(def);
		}
	}
}

Jresult *
jq_exec(Jconn * conn, const char *query)
{
	jstring		statement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, errmsg("In jq_exec(%p): %s", conn, query));

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	/* The query argument */
	statement = (*Jenv)->NewStringUTF(Jenv, query);
	if (statement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to create query argument")));
	}
	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->createStatement, statement);
	jq_get_exception();
	/* Return Java memory */
	(*Jenv)->DeleteLocalRef(Jenv, statement);
	*res = PGRES_COMMAND_OK;
	return res;
}

Jresult *
jq_exec_id(Jconn * conn, const char *query, int *resultSetID)
{
	jstring		statement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, (errmsg("In jq_exec_id(%p): %s", conn, query)));

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	/* The query argument */
	statement = (*Jenv)->NewStringUTF(Jenv, query);
	if (statement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to create query argument")));
	}
	jq_exception_clear();
	*resultSetID = (int) (*Jenv)->CallIntMethod(Jenv, conn->JDBCUtilsObject, jni_methods->createStatementID, statement);
	jq_get_exception();
	if (*resultSetID < 0)
	{
		(*Jenv)->DeleteLocalRef(Jenv, statement);
		ereport(ERROR, (errmsg("Get resultSetID failed with code: %d", *resultSetID)));
	}
	ereport(DEBUG3, (errmsg("Get resultSetID successfully, ID: %d", *resultSetID)));

	/* Return Java memory */
	(*Jenv)->DeleteLocalRef(Jenv, statement);
	*res = PGRES_COMMAND_OK;
	return res;
}

void *
jq_release_resultset_id(Jconn * conn, int resultSetID)
{
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;

	ereport(DEBUG3, (errmsg("In jq_release_resultset_id: %d", resultSetID)));

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->clearResultSetID, resultSetID);
	jq_get_exception();

	return NULL;
}

/*
 * jq_iterate: Read the next row from the remote server
 */
TupleTableSlot *
jq_iterate(Jconn * conn, ForeignScanState * node, List * retrieved_attrs, int resultSetID)
{
	jobject		JDBCUtilsObject;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	jclass		JDBCUtilsClass;
	jobjectArray rowArray;
	char	  **values;
	int			numberOfColumns;

	ereport(DEBUG3, (errmsg("In jq_iterate")));

	memset(tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset(tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	ExecClearTuple(tupleSlot);
	jdbc_sig_int_interrupt_check_process();

	jq_exception_clear();
	numberOfColumns = (int) (*Jenv)->CallIntMethod(Jenv, conn->JDBCUtilsObject, jni_methods->getNumberOfColumns, resultSetID);
	jq_get_exception();
	if (numberOfColumns < 0)
	{
		ereport(ERROR, (errmsg("getNumberOfColumns got wrong value: %d", numberOfColumns)));
	}

	if ((*Jenv)->PushLocalFrame(Jenv, (numberOfColumns + 10)) < 0)
	{
		ereport(ERROR, (errmsg("Error pushing local java frame")));
	}

	/* Allocate pointers to the row data */
	jq_exception_clear();
	rowArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, jni_methods->getResultSet, resultSetID);
	jq_get_exception();
	if (rowArray != NULL)
	{
		if(retrieved_attrs != NIL) {

			values = (char **) palloc0(tupleDescriptor->natts * sizeof(char *));
			for (int i = 0; i < retrieved_attrs->length; i++)
			{
				int			column_index = retrieved_attrs->elements[i].int_value - 1;
				Oid			pgtype = TupleDescAttr(tupleDescriptor, column_index)->atttypid;
				int32		pgtypmod = TupleDescAttr(tupleDescriptor, column_index)->atttypmod;
				jobject		obj = (jobject) (*Jenv)->GetObjectArrayElement(Jenv, rowArray, i);

				if (obj != NULL)
				{
					tupleSlot->tts_isnull[column_index] = false;
					tupleSlot->tts_values[column_index] = jdbc_convert_object_to_datum(pgtype, pgtypmod, obj);
				}
			}
		} else {
			jsize size = (*Jenv)->GetArrayLength(Jenv, rowArray);
			memset(tupleSlot->tts_values, 0, sizeof(Datum) * (int)size);
			memset(tupleSlot->tts_isnull, true, sizeof(bool) * (int)size);
			ExecClearTuple(tupleSlot);
			values = (char **) palloc0(size * sizeof(char *));
			for (int i = 0; i < size; i++)
			{
				values[i] = jdbc_convert_string_to_cstring((jobject) (*Jenv)->GetObjectArrayElement(Jenv, rowArray, i));
				if (values[i] != NULL)
				{
					tupleSlot->tts_isnull[i] = false;
					tupleSlot->tts_values[i] = *values[i];
				}
			}
		}
		ExecStoreVirtualTuple(tupleSlot);
		(*Jenv)->DeleteLocalRef(Jenv, rowArray);
	}
	(*Jenv)->PopLocalFrame(Jenv, NULL);
	return (tupleSlot);
}


Jresult *
jq_exec_prepared(Jconn * conn, const int *paramLengths,
				 const int *paramFormats, int resultFormat, int resultSetID)
{
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, (errmsg("In jq_exec_prepared")));

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	jq_exception_clear();
	(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->execPreparedStatement, resultSetID);
	jq_get_exception();

	/* Return Java memory */
	*res = PGRES_COMMAND_OK;

	return res;
}

void
jq_clear(Jresult * res)
{
	ereport(DEBUG3, (errmsg("In jq_clear")));
	pfree(res);
	return;
}

char *
jq_cmd_tuples(Jresult * res)
{
	ereport(DEBUG3, (errmsg("In jq_cmd_tuples")));
	return 0;
}

char *
jq_get_value(const Jresult * res, int tup_num, int field_num)
{
	ereport(DEBUG3, (errmsg("In jq_get_value")));
	return 0;
}

Jresult *
jq_prepare(Jconn * conn, const char *query,
		   const Oid * paramTypes, int *resultSetID)
{
	jstring		statement;
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult    *res;

	ereport(DEBUG3, (errmsg("In jq_prepare(%p): %s", conn, query)));

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	/* The query argument */
	statement = (*Jenv)->NewStringUTF(Jenv, query);
	if (statement == NULL)
	{
		ereport(ERROR, (errmsg("Failed to create query argument")));
	}
	jq_exception_clear();
	/* get the resultSetID */
	*resultSetID = (int) (*Jenv)->CallIntMethod(Jenv, conn->JDBCUtilsObject, jni_methods->createPreparedStatement, statement);
	jq_get_exception();
	if (*resultSetID < 0)
	{
		/* Return Java memory */
		(*Jenv)->DeleteLocalRef(Jenv, statement);
		ereport(ERROR, (errmsg("Get resultSetID failed with code: %d", *resultSetID)));
	}
	ereport(DEBUG3, (errmsg("Get resultSetID successfully, ID: %d", *resultSetID)));

	/* Return Java memory */
	(*Jenv)->DeleteLocalRef(Jenv, statement);
	*res = PGRES_COMMAND_OK;

	return res;
}

int
jq_nfields(const Jresult * res)
{
	ereport(DEBUG3, (errmsg("In jq_nfields")));
	return 0;
}

int
jq_get_is_null(const Jresult * res, int tup_num, int field_num)
{
	ereport(DEBUG3, (errmsg("In jq_get_is_null")));
	return 0;
}

Jconn *
jq_connect_db_params(const ForeignServer * server, const UserMapping * user,
					 const char *const *keywords, const char *const *values)
{
	Jconn	   *conn;
	int			i = 0;

	ereport(DEBUG3, (errmsg("In jq_connect_db_params")));
	while (keywords[i])
	{
		const char *pvalue = values[i];

        // FIXME: null pointer dereference
		if (pvalue == NULL && pvalue[0] == '\0')
		{
			break;
		}
		i++;
	}
	/* Initialize the Java JVM (if it has not been done already) */
	jdbc_jvm_init(server, user);
	conn = jdbc_create_JDBC_connection(server, user);
	if (jq_status(conn) == CONNECTION_BAD)
	{
		(void) jdbc_connect_db_complete(conn);
	}
	return conn;
}

/*
 * Do any cleanup needed and close a database connection Return 1 on success,
 * 0 on failure
 */
static int
jdbc_connect_db_complete(Jconn * conn)
{
	ereport(DEBUG3, (errmsg("In jdbc_connect_db_complete")));
	return 0;
}

ConnStatusType
jq_status(const Jconn * conn)
{
	if (!conn)
	{
		return CONNECTION_BAD;
	}
	return conn->status;
}

char *
jq_error_message(const Jconn * conn)
{
	ereport(DEBUG3, (errmsg("In jq_error_message")));
	return "Unknown Error!";
}

int
jq_connection_used_password(const Jconn * conn)
{
	ereport(DEBUG3, (errmsg("In jq_connection_used_password")));
	return 0;
}

void
jq_finish(Jconn * conn)
{
	ereport(DEBUG3, (errmsg("In jq_finish for conn=%p", conn)));
	jdbc_detach_jvm();
	conn = NULL;
	return;
}

int
jq_server_version(const Jconn * conn)
{
	ereport(DEBUG3, (errmsg("In jq_server_version")));
	return 0;
}

char *
jq_result_error_field(const Jresult * res, int fieldcode)
{
	ereport(DEBUG3, (errmsg("In jq_result_error_field")));
	return 0;
}

PGTransactionStatusType
jq_transaction_status(const Jconn * conn)
{
	ereport(DEBUG3, (errmsg("In jq_transaction_status")));
	return PQTRANS_UNKNOWN;
}
void *
jq_bind_sql_var(Jconn * conn, Oid type, int attnum, Datum value, bool *isnull, int resultSetID)
{
	jclass		JDBCUtilsClass;
	jobject		JDBCUtilsObject;
	Jresult	   *res;

	ereport(DEBUG3, (errmsg("In jq_bind_sql_var")));

	res = (Jresult *) palloc0(sizeof(Jresult));
	*res = PGRES_FATAL_ERROR;

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	attnum++;
	ereport(DEBUG2, errmsg("jdbc_fdw : %s %d type=%u ", __func__, attnum, type));

	if (*isnull)
	{
		jq_exception_clear();
		(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindNullPreparedStatement, attnum, resultSetID);
		jq_get_exception();
		*res = PGRES_COMMAND_OK;
		return NULL;
	}

	switch (type)
	{
		case INT2OID:
			{
				int16		dat = DatumGetInt16(value);

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindIntPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case INT4OID:
			{
				int32		dat = DatumGetInt32(value);

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindIntPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case INT8OID:
			{
				int64		dat = DatumGetInt64(value);

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindLongPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}

		case FLOAT4OID:

			{
				float4		dat = DatumGetFloat4(value);

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindFloatPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case FLOAT8OID:
			{
				float8		dat = DatumGetFloat8(value);

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindDoublePreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}

		case NUMERICOID:
			{
				Datum		valueDatum = DirectFunctionCall1(numeric_float8, value);
				float8		dat = DatumGetFloat8(valueDatum);

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindDoublePreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case BOOLOID:
			{
				bool		dat = (bool) value;

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindBooleanPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();
				break;
			}

		case BYTEAOID:
			{
				long		len;
				char	   *dat = NULL;
				char	   *result = DatumGetPointer(value);
				jbyteArray	retArray;

				if (VARATT_IS_1B(result))
				{
					len = VARSIZE_1B(result) - VARHDRSZ_SHORT;
					dat = VARDATA_1B(result);
				}
				else
				{
					len = VARSIZE_4B(result) - VARHDRSZ;
					dat = VARDATA_4B(result);
				}

				retArray = (*Jenv)->NewByteArray(Jenv, len);
				(*Jenv)->SetByteArrayRegion(Jenv, retArray, 0, len, (jbyte *) (dat));


				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindByteaPreparedStatement, retArray, len, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		case NAMEOID:
			{
				/* Bind as text */
				char	   *outputString = NULL;
				jstring		dat = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				dat = (*Jenv)->NewStringUTF(Jenv, outputString);
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindStringPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();

				(*Jenv)->DeleteLocalRef(Jenv, dat);
				break;
			}
		case TIMEOID:
			{
				/* Bind as text */
				char	   *outputString = NULL;
				jstring		dat = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				dat = (*Jenv)->NewStringUTF(Jenv, outputString);
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindTimePreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();

				/* Return Java memory */
				(*Jenv)->DeleteLocalRef(Jenv, dat);
				break;
			}
		case TIMETZOID:
			{
				/* Bind as text */
				char	   *outputString = NULL;
				jstring		dat = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				dat = (*Jenv)->NewStringUTF(Jenv, outputString);
				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindTimeTZPreparedStatement, dat, attnum, resultSetID);
				jq_get_exception();

				/* Return Java memory */
				(*Jenv)->DeleteLocalRef(Jenv, dat);
				break;
			}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				/*
				 * Bind as microseconds from Unix Epoch time in UTC time zone
				 * to avoid being affected by JVM's time zone.
				 */
				Timestamp	valueTimestamp = DatumGetTimestamp(value);		/* Already in UTC time zone */
				int64		valueMicroSecs = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS;

				jq_exception_clear();
				(*Jenv)->CallObjectMethod(Jenv, conn->JDBCUtilsObject, jni_methods->bindTimestampPreparedStatement, valueMicroSecs, attnum, resultSetID);
				jq_get_exception();
				break;
			}
		default:
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("cannot convert constant value to JDBC value %u", type),
								errhint("Constant value data type: %u", type)));
				break;
			}
	}
	*res = PGRES_COMMAND_OK;
	return 0;
}

/*
 * jdbc_convert_to_pg: Convert JDBC data into PostgreSQL's compatible data
 * types
 */
Datum
jdbc_convert_to_pg(Oid pgtyp, int pgtypmod, char *value)
{
	Datum		valueDatum;
	Datum		stringDatum;
	regproc		typeinput;
	HeapTuple	tuple;
	int			typemod;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR, errmsg("cache lookup failed for type %u", pgtyp));

	typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typinput;
	typemod = ((Form_pg_type) GETSTRUCT(tuple))->typtypmod;
	ReleaseSysCache(tuple);

	stringDatum = CStringGetDatum(value);
	valueDatum = OidFunctionCall3(typeinput, stringDatum,
								   ObjectIdGetDatum(pgtyp),
								   Int32GetDatum(typemod));

	return valueDatum;
}

/*
 * jq_exception_clear: clears any exception that is currently being thrown
 */
void
jq_exception_clear()
{
	(*Jenv)->ExceptionClear(Jenv);
	return;
}

/*
 * jq_get_exception: get the JNI exception is currently being thrown convert
 * to String for ouputing error message
 */
void
jq_get_exception()
{
	/* check for pending exceptions */
	if ((*Jenv)->ExceptionCheck(Jenv))
	{
		jthrowable	exc;
		jmethodID	exceptionMsgID;
		jclass		objectClass;
		jstring		exceptionMsg;
		char	   *exceptionString;
		char	   *err_msg = NULL;

		/* determines if an exception is being thrown */
		exc = (*Jenv)->ExceptionOccurred(Jenv);
		/* get to the message and stack trace one as String */
		objectClass = (*Jenv)->FindClass(Jenv, "java/lang/Object");
		if (objectClass == NULL)
		{
			ereport(ERROR, (errmsg("java/lang/Object class could not be created")));
		}
		exceptionMsgID = (*Jenv)->GetMethodID(Jenv, objectClass, "toString", "()Ljava/lang/String;");
		exceptionMsg = (jstring) (*Jenv)->CallObjectMethod(Jenv, exc, exceptionMsgID);

		exceptionString = jdbc_convert_string_to_cstring((jobject) exceptionMsg);
		err_msg = pstrdup(exceptionString);
		ereport(ERROR, errmsg("remote server returned an error"));
		ereport(DEBUG3, errmsg("%s", err_msg));

	}
	return;
}

static List *
jq_get_column_infos(Jconn * conn, char *tablename)
{
	jobject		JDBCUtilsObject;
	jclass		JDBCUtilsClass;
	jstring		jtablename = (*Jenv)->NewStringUTF(Jenv, tablename);

	/* getColumnNames */
	jobjectArray columnNamesArray;
	jsize		numberOfNames;

	/* getColumnTypes */
	jobjectArray columnTypesArray;
	jsize		numberOfTypes;

	/* getPrimaryKey */
	jobjectArray primaryKeyArray;
	jsize		numberOfKeys;
	List	   *primaryKey = NIL;

	/* for generating columnInfo List */
	List	   *columnInfoList = NIL;
	JcolumnInfo *columnInfo;
	ListCell   *lc;

	/* Get JDBCUtils */
	PG_TRY();
	{
		jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);
	}
	PG_CATCH();
	{
		(*Jenv)->DeleteLocalRef(Jenv, jtablename);
		PG_RE_THROW();
	}
	PG_END_TRY();

	jdbc_sig_int_interrupt_check_process();
	/* getColumnNames */
	jq_exception_clear();
	columnNamesArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, jni_methods->getColumnNames, jtablename);
	jq_get_exception();
	/* getColumnTypes */
	jq_exception_clear();
	columnTypesArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, jni_methods->getColumnTypes, jtablename);
	jq_get_exception();
	/* getPrimaryKey */
	jq_exception_clear();
	primaryKeyArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, jni_methods->getPrimaryKey, jtablename);
	jq_get_exception();
	if (primaryKeyArray != NULL)
	{
		numberOfKeys = (*Jenv)->GetArrayLength(Jenv, primaryKeyArray);
		for (int i = 0; i < numberOfKeys; i++)
		{
			char	   *tmpPrimaryKey = jdbc_convert_string_to_cstring((jobject) (*Jenv)->GetObjectArrayElement(Jenv, primaryKeyArray, i));

			primaryKey = lappend(primaryKey, tmpPrimaryKey);
		}
		(*Jenv)->DeleteLocalRef(Jenv, primaryKeyArray);
	}

	if (columnNamesArray != NULL && columnTypesArray != NULL)
	{
		numberOfNames = (*Jenv)->GetArrayLength(Jenv, columnNamesArray);
		numberOfTypes = (*Jenv)->GetArrayLength(Jenv, columnTypesArray);

		if (numberOfNames != numberOfTypes)
		{
			(*Jenv)->DeleteLocalRef(Jenv, jtablename);
			(*Jenv)->DeleteLocalRef(Jenv, columnTypesArray);
			(*Jenv)->DeleteLocalRef(Jenv, columnNamesArray);
			ereport(ERROR, (errmsg("Cannot get the dependable columnInfo.")));
		}

		for (int i = 0; i < numberOfNames; i++)
		{
			/* init columnInfo */
			char	   *tmpColumnNames = jdbc_convert_string_to_cstring((jobject) (*Jenv)->GetObjectArrayElement(Jenv, columnNamesArray, i));
			char	   *tmpColumnTypes = jdbc_convert_string_to_cstring((jobject) (*Jenv)->GetObjectArrayElement(Jenv, columnTypesArray, i));

			columnInfo = (JcolumnInfo *) palloc0(sizeof(JcolumnInfo));
			columnInfo->column_name = tmpColumnNames;
			columnInfo->column_type = tmpColumnTypes;
			columnInfo->primary_key = false;
			/* check the column is primary key or not */
			foreach(lc, primaryKey)
			{
				char	   *tmpPrimaryKey = NULL;

				tmpPrimaryKey = (char *) lfirst(lc);
				if (!strcmp(tmpPrimaryKey, tmpColumnNames))
				{
					columnInfo->primary_key = true;
				}
			}
			columnInfoList = lappend(columnInfoList, columnInfo);
		}
	}

	if (columnNamesArray != NULL)
	{
		(*Jenv)->DeleteLocalRef(Jenv, columnNamesArray);
	}
	if (columnTypesArray != NULL)
	{
		(*Jenv)->DeleteLocalRef(Jenv, columnTypesArray);
	}
	(*Jenv)->DeleteLocalRef(Jenv, jtablename);

	return columnInfoList;
}


/*
 * jq_get_table_names
 */
static List *
jq_get_table_names(Jconn * conn)
{
	jobject		JDBCUtilsObject;
	jclass		JDBCUtilsClass;
	jobjectArray tableNameArray;
	List	   *tableName = NIL;
	jsize		numberOfTables;

	jq_get_JDBCUtils(conn, &JDBCUtilsClass, &JDBCUtilsObject);

	jdbc_sig_int_interrupt_check_process();
	jq_exception_clear();
	tableNameArray = (*Jenv)->CallObjectMethod(Jenv, JDBCUtilsObject, jni_methods->getTableNames);
	jq_get_exception();
	if (tableNameArray != NULL)
	{
		numberOfTables = (*Jenv)->GetArrayLength(Jenv, tableNameArray);
		for (int i = 0; i < numberOfTables; i++)
		{
			char	   *tmpTableName = jdbc_convert_string_to_cstring((jobject) (*Jenv)->GetObjectArrayElement(Jenv, tableNameArray, i));

			tableName = lappend(tableName, tmpTableName);
		}
		(*Jenv)->DeleteLocalRef(Jenv, tableNameArray);
	}
	return tableName;
}

/*
 * jq_get_schema_info
 */
List *
jq_get_schema_info(Jconn * conn)
{
	List	   *schema_list = NIL;
	List	   *tableName = NIL;
	JtableInfo *tableInfo;
	ListCell   *lc;

	tableName = jq_get_table_names(conn);

	foreach(lc, tableName)
	{
		char	   *tmpTableName = NULL;

		tmpTableName = (char *) lfirst(lc);
		tableInfo = (JtableInfo *) palloc0(sizeof(JtableInfo));
		if (tmpTableName != NULL)
		{
			tableInfo->table_name = tmpTableName;
			tableInfo->column_info = jq_get_column_infos(conn, tmpTableName);
			schema_list = lappend(schema_list, tableInfo);
		}
	}
	return schema_list;
}

/*
 * jq_get_JDBCUtils: get JDBCUtilsClass and JDBCUtilsObject
 */
static void
jq_get_JDBCUtils(Jconn *conn, jclass *JDBCUtilsClass, jobject *JDBCUtilsObject)
{
	/* Our object of the JDBCUtils class is on the connection */
	*JDBCUtilsObject = conn->JDBCUtilsObject;
	if (*JDBCUtilsObject == NULL)
	{
		ereport(ERROR, (errmsg("Cannot get the utilsObject from the connection")));
	}
	*JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
	if (*JDBCUtilsClass == NULL)
	{
		ereport(ERROR, (errmsg("JDBCUtils class could not be created")));
	}
}

// internal helper for jq_load_jni_methods
static jmethodID find_jni_method_or_fail(jclass JDBCUtilsClass, char *name, char *signature)
{
	jmethodID   method;

	jdbc_sig_int_interrupt_check_process();
	method = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, name, signature);
	if (method == NULL)
	{
		ereport(ERROR, (errmsg("Failed to find the method %s", name)));
	}
    return method;
}

static void jq_load_jni_methods(jclass utils_class, JNIMethods *methods)
{
    methods->createConnection = find_jni_method_or_fail(utils_class, "createConnection", "(I[Ljava/lang/String;)V");
    methods->createStatement = find_jni_method_or_fail(utils_class, "createStatement", "(Ljava/lang/String;)V");
    methods->createStatementID = find_jni_method_or_fail(utils_class, "createStatementID", "(Ljava/lang/String;)I");
    methods->clearResultSetID = find_jni_method_or_fail(utils_class, "clearResultSetID", "(I)V");
    methods->createPreparedStatement = find_jni_method_or_fail(utils_class, "createPreparedStatement", "(Ljava/lang/String;)I");
    methods->execPreparedStatement = find_jni_method_or_fail(utils_class, "execPreparedStatement", "(I)V");
    methods->getResultSet = find_jni_method_or_fail(utils_class, "getResultSet", "(I)[Ljava/lang/Object;");
    methods->getNumberOfColumns = find_jni_method_or_fail(utils_class, "getNumberOfColumns", "(I)I");
    methods->getTableNames = find_jni_method_or_fail(utils_class, "getTableNames", "()[Ljava/lang/String;");
    methods->getColumnNames = find_jni_method_or_fail(utils_class, "getColumnNames", "(Ljava/lang/String;)[Ljava/lang/String;");
    methods->getColumnTypes = find_jni_method_or_fail(utils_class, "getColumnTypes", "(Ljava/lang/String;)[Ljava/lang/String;");
    methods->getPrimaryKey = find_jni_method_or_fail(utils_class, "getPrimaryKey", "(Ljava/lang/String;)[Ljava/lang/String;");
    methods->cancel = find_jni_method_or_fail(utils_class, "cancel", "()V");
    methods->getIdentifierQuoteString = find_jni_method_or_fail(utils_class, "getIdentifierQuoteString", "()Ljava/lang/String;");

    methods->bindNullPreparedStatement = find_jni_method_or_fail(utils_class, "bindNullPreparedStatement", "(II)V");
    methods->bindIntPreparedStatement = find_jni_method_or_fail(utils_class, "bindIntPreparedStatement", "(III)V");
    methods->bindLongPreparedStatement = find_jni_method_or_fail(utils_class, "bindLongPreparedStatement", "(JII)V");
    methods->bindFloatPreparedStatement = find_jni_method_or_fail(utils_class, "bindFloatPreparedStatement", "(FII)V");
    methods->bindDoublePreparedStatement = find_jni_method_or_fail(utils_class, "bindDoublePreparedStatement", "(DII)V");
    methods->bindBooleanPreparedStatement = find_jni_method_or_fail(utils_class, "bindBooleanPreparedStatement", "(ZII)V");
    methods->bindStringPreparedStatement = find_jni_method_or_fail(utils_class, "bindStringPreparedStatement",
        "(Ljava/lang/String;II)V");
    methods->bindByteaPreparedStatement = find_jni_method_or_fail(utils_class, "bindByteaPreparedStatement", "([BJII)V");
    methods->bindTimePreparedStatement = find_jni_method_or_fail(utils_class, "bindTimePreparedStatement",
        "(Ljava/lang/String;II)V");
    methods->bindTimeTZPreparedStatement = find_jni_method_or_fail(utils_class, "bindTimeTZPreparedStatement",
	    "(Ljava/lang/String;II)V");
    methods->bindTimestampPreparedStatement = find_jni_method_or_fail(utils_class, "bindTimestampPreparedStatement",
        "(JII)V");
}