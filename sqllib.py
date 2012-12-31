#!/usr/bin/python

import re
import warnings
from threading import Thread
from Queue import Queue
import os, sys

version=(0,2,20100119)

debug=False

class Error(Exception): pass
class NoTableError(Error): pass
class QueryError(Error): pass
class NoKeysError(Error):  pass

class SQLResult(object):
	__slots__=['_pos','_dictlist','count','results','lastrowid','sql','args','cols','table']
	def __repr__(self):
		return "<%s.%s object at %s c=%d%s>"%(self.__class__.__module__,self.__class__.__name__,hash(self),self.count," from %s"%self.table if hasattr(self,"table") else "")
	def _get_pos(self):
		try: self._pos
		except AttributeError: self._pos=dict([(x,idx) for idx,x in enumerate(self.cols)])
		return self._pos
	def _get_dictlist(self):
		try: self._dictlist
		except AttributeError:
			self._dictlist=[dict((y,x[idx]) for idx,y in enumerate(self.cols)) for x in self.results]
		return self._dictlist
	pos=property(_get_pos)
	col1=property(lambda self: [x[0] for x in self.results])
	scalar=property(lambda self: self.results[0][0])
	dictlist=property(_get_dictlist)
	def __nonzero__(self):
		if self.results or self.count>0: return True
		return False
	def __getitem__(self,key):
		if isinstance(key, (int,long)):
			return self.results[key]
		elif key in self.pos:
			idx=self.pos[key]
			return [row[idx] for row in self]
		else: raise ValueError("lookup key needs to be row index or column name")
	def __init__(self,sql,*args):
		self.sql=sql
		self.args=args
	def __iter__(self):
		for row in self.results:
			yield row
	def __len__(self):
		if self.results is not None: return len(self.results)
		else: return self.count

class SQLTable(object):
	class ContainsCheck(object):
		def __init__(self,table,cond):
			self.table=table
			self.cond=cond
		def __contains__(self,vals):
			return True if self.table.select("1",self.cond,*vals) else False
	def __getattr__(self,key):
		if key=='keys':
			self.keys=self.get_keys()
			return self.keys
		if key=='cols':
			self.cols=self.select('*',cond='1=0').cols
			return self.cols
		raise AttributeError
	def __init__(self,name,conn):
		self.conn=conn
		self.name=name
	def get_keys(self): return []
	def create(self,coldef): self.conn.create_table(self.name,coldef)
	def select(self, cols='*', cond=None, *args, **kwargs):
		return self.conn.select(self.name, cols, cond, *args, **kwargs)
	__call__=select
	def insert(self,inf, **kwargs): return self.conn.insert(self.name,inf, **kwargs)
	def update(self,inf,cond,*args): return self.conn.update(self.name,inf,cond,*args)
	def delete(self,cond,*args): return self.conn.delete(self.name,cond,*args)
	def __contains__(self,cond):
		return True if self.select("1",cond) else False
	def mk_check(self,cond):
		return self.ContainsCheck(self,cond)
	def set_sepval(self, inf, cond, sep=" "):
		for col,val in inf.iteritems():
			for v in self("distinct %s"%col, cond).col1:
				if v: nv=sep.join(sorted(set([x for x in v.split(sep) if x]+[val])))
				else: nv=val
				if nv!=v:
					self.update({col:nv},And(cond, {col:v}))
	def set(self,inf,keys=None,overwrite={}):
		if keys is None: keys=self.keys
		if keys:
			prims=[x for x in inf if x in keys]
			non_prims=[x for x in inf if x not in keys]
			prim_cond=['%s=%s'%(x,self.conn.api.p) for x in prims]+[self.conn.api.isnull_cond(x) for x in keys if x not in prims]
			prim_args=[inf[x] for x in prims]
			if non_prims:
				exist=self.select(['%s as %s'%(self.conn.api.isnull_cond(x),x) for x in non_prims],prim_cond,*prim_args)
			else:
				exist=self.select(['1'],prim_cond,*prim_args)
			if exist:
				if non_prims:
					row=exist[0]
					new_inf=dict([(x,inf[x]) for x in non_prims if (row[exist.pos[x]] and inf[x] is not None) or x in overwrite])
					if new_inf:
						self.update(new_inf,prim_cond,*prim_args)
			else:
				self.insert(inf)
		else:
			raise NoKeysError,"No set for table(%s) without unique keys"%(self.name)

class DB_API(object):
	verbose=1
	filename_pat=None
	escapechar='\\';
	identifier_quotechar="`"
	oidstr="OID,"
	p='%s'
	sql_tablename_re=re.compile(r'^(?:update|insert\s+into|(?:(?:delete|select.*?)\s+from))\s+(?P<quote>"?)(?P<table_name>[^\s",]+)(?P=quote)',re.I)
	class Result(SQLResult):
		__slots__=[]
		cursor_rowid_attr="lastrowid"
		def parse_cursor(self,cursor):
			self.count=cursor.rowcount
			if cursor.description:
				self.results=cursor.fetchall()
				self.cols=[x[0] for x in cursor.description]
			else:
				self.results=None
				self.cols=None
			if self.cursor_rowid_attr: self.lastrowid=getattr(cursor,self.cursor_rowid_attr)
	class Table(SQLTable):
		def get_keys(self):
			return self.conn("SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME=%s",self.name).col1
	def isnull_cond(self,col):
		return '%s IS NULL'%(col)
	def scalar(self,sql,*args):
		cursor=self.connection.cursor()
		cursor.execute(sql,args)
		ret=cursor.fetchone()[0]
		cursor.close()
		return ret
	def close(self):
		self.connection.close()
	def escape(self,val):
		if val is None: return "NULL"
		elif type(val) in (str,unicode):
			if self.escapechar: val=val.replace(self.escapechar,'%s%s'%(self.escapechar,self.escapechar))
			return (u"'%s'" if isinstance(val,unicode) else "'%s'")%(val.replace("'","''"))
		elif type(val) in (int,long): return "%d"%(val)
		elif type(val)==float: return "%f"%(val)
		elif type(val)==buffer: return "X'%s'"%(str(val).encode('hex'))
		else: return self.escape(str(val))
	def table_names(self,*args,**kwargs): 
		raise NotImplementedError("%s API does not implement table_names"%(self.__class__.__name__))
	def has_table(self,name):
		return name in self.table_names()
	def __call__(self,sql,*args):
		if self.verbose>1: print 'Execute: %r, %r'%(sql,args)
		ret=self.Result(sql,*args)
		m=self.sql_tablename_re.match(sql)
		if m: ret.table=m.group("table_name")
		cursor=self.connection.cursor()
		if args: cursor.execute(sql,args)
		else: cursor.execute(sql)
		ret.parse_cursor(cursor)
		if self.verbose>0 and ret.count and ret.count>0 and (sql.lower().startswith("update ") or sql.lower().startswith("insert ") or sql.lower().startswith("delete ")):
			print '%d rows affected by %s'%(ret.count,sql),args
		cursor.close()
		return ret

class SQLite_API(DB_API):
	unique_re=re.compile(r'\bunique\s*\((.*?)\)',re.I)
	filename_pat='%s'
	escapechar=''
	p='?'
	class Result(SQLResult):
		__slots__=[]
		def parse_cursor(self,cursor):
			self.count=cursor.rowcount
			if cursor.description:
				self.results=cursor.fetchall()
				self.cols=[x[0] for x in cursor.description]
			else:
				self.results=None
				self.cols=None
			self.lastrowid=cursor.lastrowid
	class Table(SQLTable):
		def get_keys(self):
			m=self.conn.api.unique_re.search(self.conn.api.scalar('SELECT sql FROM sqlite_master WHERE type=? AND name=?','table',self.name))
			if m: return re.sub('\s','',m.group(1)).split(',')
	def __init__(self,database):
		try: import pysqlite2.dbapi2 as api
		except ImportError: import sqlite3.dbapi2 as api
		if map(int,api.version.split('.'))<[2,3,3]:
			raise Error,"SQLite old buggy API, upgrade to at least 2.3.3"
		self.dbapi=api
		self.connection=api.connect(database,isolation_level=None)
		self.connection.text_factory=str
		try:
			self.connection.create_function("REGEXP", 2, lambda expr,item: re.search(expr,item) is not None)
			self.connection.create_function("REGEXP", 3, lambda data,pat,repl: re.sub(pat,repl,data))
		except Exception,e:
			print >>sys.stderr, "Could not create REGEXP function"
	def escape_string(self,s):
		return s.replace("'","''")
	def table_names(self):
		return self("SELECT name FROM sqlite_master WHERE type in ('table','view')").col1
	def has_table(self,name):
		if self('SELECT 1 FROM sqlite_master WHERE type=? AND name=?','table',name): return True
		else: return False
	def __call__(self,sql,*args):
		if self.verbose>1: print 'Execute:',sql,args
		res=self.Result(sql,*args)
		try: cursor=self.connection.execute(sql,args)
		except self.dbapi.Error,e: raise QueryError,(e,sql,args)
		res.parse_cursor(cursor)
		if res.count and res.count>0 and self.verbose>0: print '%d rows affected by %s'%(res.count,sql),args
		return res

class SQLite_Thread_API(SQLite_API):
	def call_loop(self,*args,**kwargs):
		SQLite_API.__init__(self,*args,**kwargs)
		while self.running:
			c=self.cq.get()
			if debug: print "executing call %r"%(c,)
			mode,(sql,args)=c
			if mode=='call':
				try: ret=SQLite_API.__call__(self,sql,*args)
				except Exception,e: ret=e
				self.rq.put(ret)
			elif mode=='scalar':
				try: ret=SQLite_API.scalar(self,sql,*args)
				except Exception,e: ret=e
				self.rq.put(ret)
			elif mode=='stop':
				if debug: print "Empty call"
			else: raise SyntaxError,"Unknown mode: %s"%(mode)
	def __init__(self,*args,**kwargs):
		import thread
		from Queue import Queue
		self.cq=Queue(1)
		self.rq=Queue(1)
		self.running=True
		self.in_call=thread.allocate_lock()
		self.callthread=thread.start_new_thread(self.call_loop,args,kwargs)
	def __del__(self):
		self.running=False
		self.cq.put(('stop',(None,())))
	def scalar(self,sql,*args):
		self.in_call.acquire()
		self.cq.put(('scalar',(sql,args)))
		ret=self.rq.get()
		self.in_call.release()
		if isinstance(ret,Exception): raise ret
		return ret
	def __call__(self,sql,*args):
		self.in_call.acquire()
		self.cq.put(('call',(sql,args)))
		ret=self.rq.get()
		self.in_call.release()
		if isinstance(ret,Exception): raise ret
		return ret

class MySQL_API(DB_API):
	oidstr=""
	def __init__(self,connstr):
		import MySQLdb as api
		self.dbapi=api
		connparam=dict([x.split('=',2) for x in connstr.split(',')])
		self.connection=api.connect(**connparam)
		self.connection.set_character_set('utf8')
	def table_names(self):
		cursor=self.connection.cursor()
		cursor.execute("show tables")
		ret=[x[0] for x in cursor.fetchall()]
		cursor.close()
		return ret

class Postgres_API(DB_API):
	oidstr=""
	insert_returning=True
	identifier_quotechar='"'
	class CopyFromProducer(Thread):
		daemon=True
		def __init__(self, iter_obj, fobj):
			self.iter_obj=iter_obj
			self.fobj=fobj
			Thread.__init__(self)
		def run(self):
			while True:
				try: row=self.iter_obj.next()
				except StopIteration: break
				print >>self.fobj,"\t".join(map(lambda d: "\\N" if d is None else d.encode("string_escape") if isinstance(d,str) else str(d), row))
			self.fobj.close()
	def __init__(self, connstr):
		import psycopg2 as api
		import psycopg2.extensions
		psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
		psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
		self.dbapi=api
		self.connection=api.connect(connstr)
		self.connection.set_isolation_level(api.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	def table_names(self):
		return self("select tablename from pg_tables where schemaname=%s union select viewname from pg_views where schemaname=%s order by 1","public","public")["tablename"]
	def __call__(self, sql, *args):
		try: 
			ret=DB_API.__call__(self, sql, *args)
		except self.dbapi.ProgrammingError:
			self("ABORT")
			raise
		else:
			if sql.lower().startswith("insert") and ret.results and ret.lastrowid==0:
				ret.lastrowid=ret.scalar
			return ret
	def copy_from(self, table, iter_obj, columns=None):
		fd_in,fd_out=os.pipe()
		f_in,f_out=os.fdopen(fd_in), os.fdopen(fd_out,"w")
		producer=self.CopyFromProducer(iter_obj, f_out)
		producer.start()
		cur=self.connection.cursor()
		try: cur.copy_from(f_in, table, columns=columns)
		except self.dbapi.ProgrammingError:
			self("ABORT")
			raise
		else:
			self.connection.commit()

class JDBC_API(DB_API):
	classpath=[]
	driver='org.hsqldb.jdbcDriver'
	catalog=None
	connprop={}
	class Result(SQLResult):
		__slots__=[]
		def parse_resultset(self,resultset):
			self.results=[]
			metadata=resultset.getMetaData()
			colcount=metadata.getColumnCount()
			self.cols=[metadata.getColumnName(x) for x in range(1,colcount+1)]
			while resultset.next():
				row=[]
				for i in range(1,colcount+1):
					val_obj=resultset.getObject(i)
					row.append(getattr(val_obj,'value',val_obj))
				self.results.append(row)
			self.count=len(self.results)
	def __init__(self,database,**params):
		for key,val in params.items(): setattr(self,key,val)
		import jpype #@UnresolvedImport
		if not jpype.isJVMStarted():
			if self.classpath: cpargs=['-Djava.class.path=%s'%':'.join(self.classpath)]
			else: cpargs=[]
			jpype.startJVM(jpype.getDefaultJVMPath(),*cpargs)
		self.jdbc=jpype.JClass(self.driver)()
		if self.connprop:
			connprop=jpype.JClass('java.util.Properties')()
			for key,val in self.connprop.items(): connprop.setProperty(key,val)
		else: connprop=None
		self.connection=self.jdbc.connect(database,connprop)
	def __call__(self,sql,*args):
		ret=self.Result(sql,*args)
		stm=self.connection.prepareStatement(sql)
		for idx,arg in enumerate(args):
			stm.setObject(idx+1,arg)
		res=stm.executeQuery()
		ret.parse_resultset(res)
		return ret
	def table_names(self):
		ret=[]
		res=self.connection.getMetaData().getTables(self.catalog,None,None,['TABLE'])
		while res.next(): ret.append(res.getString(3))
		return ret
	def has_table(self,name):
		res=self.connection.getMetaData().getTables(self.catalog,None,name,None)
		if res.next(): return res.getString(3)
		else: return False

class NameAndCond(object):
	def __init__(self,name,v):
		self.name=name
		if not isinstance(v,Condition): self.v=Eq(v)
		else: self.v=v
	def __str__(self):
		self.v.p=self.p
		return "%s%s"%(self.name,self.v)
	def args(self):
		return self.v.args()

class CondList(object):
	p="?"
	def new_elem(self,src):
		if isinstance(src,(CondList,NameAndCond)): return src
		elif isinstance(src,dict):
			ret=[]
			for k,v in src.iteritems():
				if isinstance(v,list):
					ret.append(self.__class__(*map(lambda vp: NameAndCond(k,vp),v)))
				else: ret.append(NameAndCond(k,v))
			if len(ret)==1: return ret[0]
			else: return And(*ret)
		else: raise ValueError("Unknown data type",type(src))
	def append(self,cond):
		self.elements.append(self.new_elem(cond))
	def __init__(self,*elements,**attrs):
		self.elements=map(self.new_elem,elements)
		for k,v in attrs.iteritems(): setattr(self,k,v)
	def __str__(self):
		for x in self.elements: x.p=self.p
		return "%s"%((" %s "%self.keyword).join(map(lambda x: "(%s)"%x,self.elements)))
	def args(self):
		ret=[]
		map(lambda x: ret.extend(x.args()),self.elements)
		return ret

class Or(CondList):
	keyword="OR"
class And(CondList):
	keyword="AND"

class Condition(object):
	def __init__(self,compareTo,p='?',**attr):
		self.compareTo=compareTo
		self.p=p
		for k,v in attr.iteritems(): setattr(self,k,v)
	def __str__(self): return "%s%s"%(self.op,self.p)
	def args(self):
		if self.compareTo is None: return []
		else: return [self.compareTo]
class Eq(Condition):
	def __str__(self):
		if self.compareTo is None: return ' IS NULL'
		else: return '=%s'%(self.p)
class Not(Condition):
	def __str__(self):
		if self.compareTo is None: return ' IS NOT NULL'
		elif isinstance(self.compareTo, In):
			self.compareTo.p=self.p
			return " NOT%s"%(self.compareTo)
		else: return '<>%s'%(self.p)
	def args(self):
		if isinstance(self.compareTo, In): return self.compareTo.args()
		else: return super(Not,self).args()
class Like(Condition):
	def __str__(self):
		if self.compareTo is None: return ' IS NULL'
		else: return ' like %s'%(self.p)
class NotLike(Condition):
	def __str__(self):
		if self.compareTo is None: return ' IS NOT NULL'
		else: return ' not like %s'%(self.p)
class In(Condition):
	def __str__(self):
		if self.compareTo is None: return ' IS NULL'
		else: return ' in (%s)'%(",".join([self.p for x in self.compareTo]) if isinstance(self.compareTo, (list,tuple)) else self.compareTo)
	def args(self):
		return list(self.compareTo) if isinstance(self.compareTo, (list,tuple)) else []

class DBConn(object):
	api_list=[
		('sqlite',SQLite_API),
		('mysql',MySQL_API),
		('jdbc',JDBC_API),
		('postgres',Postgres_API),
	]
	_auto_close=None
	def __getattr__(self,key):
		if key=='_tables':
			self._tables=dict([(x,self.api.Table(x,self)) for x in self.api.table_names()])
			return self._tables
		elif key in self._tables: return self[key]
		elif key.startswith('_'): raise AttributeError
		elif self.api.has_table(key):
			self._tables[key]=self.api.Table(key,self)
			return self._tables[key]
		print "No %r in DBConn"%key
		raise NoTableError,"Table %r not existng in database"%(key)
	def __getitem__(self,key):
		if key in self._tables: return self._tables[key]
		else: return self.api.Table(key, self)
	def __init__(self,database,api=None,**api_args):
		if api is None: api=self.api_list[0][1]
		elif isinstance(api, (str,unicode)): api=filter(lambda x: x[0]==api,self.api_list)[0][1]
		self.api=api(database,**api_args)
		self.verbose=1
		self._auto_close=True
	def set_verbose(self,v): self.api.verbose=v
	verbose=property(lambda self: self.api.verbose,set_verbose)
	def clear_cache(self):
		try: del self._tables
		except AttributeError: pass
	def __call__(self,sql,*args):
		return self.api(sql,*args)
	def scalar(self,tblname,cols,cond=None,*args):
		args=list(args)
		if tblname is None: tblname=""
		else: tblname=" FROM %s%s%s"%(self.api.identifier_quotechar,tblname,self.api.identifier_quotechar)
		if type(cols)==list: cols=','.join(cols)
		return self.api.scalar("SELECT %s%s%s"%(cols,tblname,self._condstr(cond,args)),*args)
	def select(self, tblname, cols, cond=None, *args, **kwargs):
		order_by=kwargs.pop("order_by",None)
		group_by=kwargs.pop("group_by", None)
		limit=kwargs.pop("limit",None)
		if kwargs:
			warnings.warn(warnings.WarningMessage("Unknown keyword args",kwargs))
		args=list(args)
		sel_tbl="" if tblname is None else " FROM %s%s%s"%(self.api.identifier_quotechar,tblname,self.api.identifier_quotechar)
		if type(cols)==list: cols=','.join(cols)
		result=self.api("SELECT %s"%"".join([
				cols,
				sel_tbl,
				self._condstr(cond, args),
				" GROUP BY %s"%group_by if group_by else "",
				" ORDER BY %s"%order_by if order_by else "",
				" LIMIT %s"%limit if limit is not None else "",
			]),*args)
		if tblname is not None: result.table=tblname
		return result
	def _condstr(self,cond,argv):
		if cond is None: return ''
		elif type(cond) in (str,unicode): return " WHERE %s"%(cond)
		elif isinstance(cond, CondList):
			cond.p=self.api.p
			argv[:0]=cond.args()
			return " WHERE %s"%cond
		elif type(cond)==dict:
			cond_list=[]
			cond_args=[]
			for k,vlist in cond.iteritems():
				if type(vlist)!=list: vlist=[vlist]
				for v in vlist:
					if isinstance(v,Condition): v.p=self.api.p
					elif isinstance(v, dict):
						if "$ne" in v: v=Not(v["$ne"], p=self.api.p)
						elif "$not" in v: v=Not(v["$not"], p=self.api.p)
						elif "$in" in v: v=In(v["$in"], p=self.api.p)
						elif "$like" in v: v=Like(v["$like"], p=self.api.p)
						else: raise ValueError("condval is dict, but no known keywords in it")
					else: v=Eq(v,p=self.api.p)
					cond_list.append('%s%s'%(k,str(v)))
					cond_args.extend(v.args())
			argv[:0]=cond_args
			return self._condstr(cond_list,argv)
		else:
			if len(cond)==0: return ''
			return " WHERE %s"%(' AND '.join(['(%s)'%x for x in cond]))
	def create_table(self,name,coldef):
		#args=list(args)
		result=self.api("CREATE TABLE %s (%s)"%(name,coldef))
		result.table=name
		return result
	def delete(self,tblname,cond,*args):
		args=list(args)
		result=self.api("DELETE FROM %s%s"%(tblname,self._condstr(cond,args)),*args)
		result.table=tblname
		return result
	def update(self,tblname,inf,cond,*args):
		args=list(args)
		keys=inf.keys()
		result=self.api("UPDATE %s SET %s%s"%(tblname,','.join(['%s=%s'%(x,self.api.p) for x in keys]),self._condstr(cond,args)),*([inf[x] for x in keys]+args))
		result.table=tblname
		return result
	def insert(self,tblname,inf, returning=None):
		if returning is not None and getattr(self.api, "insert_returning", False):
			returning=" RETURNING %s"%returning
		else: returning=""
		if type(inf)==list:
			result=self.api("INSERT INTO %s VALUES (%s)%s"%(tblname,','.join([self.api.p for x in inf]), returning), *inf)
		else:
			keys=inf.keys()
			result=self.api("INSERT INTO %s (%s) VALUES (%s)%s"%(tblname,','.join(keys), ','.join([self.api.p for x in keys]),returning), *[inf[x] for x in keys])
		result.table=tblname
		return result
	def __del__(self):
		if not self._auto_close: return
		try: self.api.close()
		except Exception,e:
			print >>sys.stderr, "Exception when closing connection:",e

if __name__=='__main__':
	import readline,rlcompleter,sys #@UnusedImport
	readline.parse_and_bind("tab: complete")
	db=DBConn(sys.argv[1])
	print "db:",db
