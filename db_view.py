#!/usr/bin/python

import sys,os
import datetime

try: mypath=os.path.dirname(__file__)
except NameError: mypath=os.path.realpath(os.path.dirname(sys.argv[0]))

version=(0,9,0,20110605)

sys.path.append(os.path.join(os.path.dirname(mypath),'lib'))
sys.path.append(os.path.join(os.path.dirname(mypath),'..','lib'))

import re,traceback,locale

from gi.repository import Gtk as gtk, GObject as gobject, Gdk  # @UnresolvedImport

import sqllib

class cached_property(object):
	_fset=_fget=None
	def __set_basic_attr(self, func):
		self.__name__=func.__name__
		self.__doc__=func.__doc__
		self.__module__=func.__module__
	@property
	def fset(self): return self._fset
	@fset.setter
	def fset(self, v):
		self._fset=v
		self.__set_basic_attr(v)
	@property
	def fget(self): return self._fget
	@fget.setter
	def fget(self, v):
		self._fget=v
		self.__set_basic_attr(v)
	def setter(self, func):
		self.fset=func
		return self
	def getter(self, func):
		self.fget=func
		return self
	def __init__(self, fget=None, fset=None):
		if fget is not None: self.fget=fget
		if fset is not None: self.fset=fset
	def __get__(self, instance, owner):
		if instance is None: return self
		try: return instance._property_cache[self.__name__]
		except AttributeError: instance._property_cache={}
		except KeyError: pass
		ret=instance._property_cache[self.__name__]=self.fget(instance)
		return ret
	def __set__(self, instance, value):
		if self.fset is not None:
			value=self._fset(instance, value)
		try: cache=instance._property_cache
		except AttributeError: cache=instance._property_cache={}
		cache[self.__name__]=value
	def __delete__(self, instance):
		try: del instance._property_cache[self.__name__]
		except (AttributeError, KeyError):
			raise AttributeError("No attribute", self)

class GtkBuilderHelper(object):
	def __init__(self,filename,cbobj=None):
		self.filename=filename
		self._ui=gtk.Builder()
		self._ui.add_from_file(filename)
		if cbobj!=None: self._ui.connect_signals(cbobj)
	def __getattr__(self,key):
		if key[0]=='_': raise AttributeError,key
		val=self._ui.get_object(key)
		if val!=None:
			setattr(self,key,val)
			return val
		raise AttributeError,"No object named '"+key+"' in %r"%(self.filename)

class Connectable(object):
	@property
	def connect_table(self):
		try: return self.__connect_table
		except AttributeError:
			ret=self.__connect_table={}
			return ret
	@connect_table.setter
	def connect_table(self, v): self.__connect_table=v
	def run_handlers(self,signal,*args,**kwargs):
		for func,add_args,add_kwargs in self.connect_table.get(signal,[]):
			func(self,*(args+add_args),**dict(kwargs,**add_kwargs))
	def connect(self,signal,func,*args,**kwargs):
		self.connect_table.setdefault(signal,[]).append((func,args,kwargs))
	def disconnect(self,signal,func):
		idx=0
		signal_table=self.connect_table[signal]
		while idx<len(signal_table):
			if signal_table[idx][0]==func: signal_table.pop(idx)
			else: idx+=1

def short_str(s,maxsize=15):
	if s is None: return ''
	if not isinstance(s,(str,unicode)): s=str(s) 
	s.replace('\r\n','\n').replace('\n',' ').replace('\r',' ')
	if len(s)>maxsize:
		s=s[:maxsize]+'..'
	return s

class Selection(object):
	__slots__=['colnr','text','rows','col']
	def __init__(self): self.reset()
	def reset(self):
		self.text=None
		self.colnr=None
		self.rows=[]
		self.col=None
	def update_selection(self,treeview,ev):
		pathinfo=treeview.get_path_at_pos(int(ev.x),int(ev.y))
		if pathinfo is None: self.reset()
		else:
			path,col,x,y=pathinfo
			for self.colnr,tcol in enumerate(treeview.get_columns()):
				if tcol is col: break
			self.col=col.get_title().replace('__','_')
			model,selected_rows=treeview.get_selection().get_selected_rows()
			self.rows=[model[x] for x in selected_rows]
			self.text=model[path][self.colnr]
	def __nonzero__(self):
		return self.colnr is not None
	def get_cross_select(self,colnr=None):
		if colnr is None: colnr=self.colnr
		return [x[colnr] for x in self.rows]

class NoKeysError(Exception): pass

class StatementInfo(object):
	search_re=re.compile(r'^\s*(?P<statement>select)\s+(?P<oid>OID,)?.+?from\s+(?P<table>\w+).*',re.I|re.S)
	_defaults=dict(table=None,sql=None, store=None,has_oids=False,is_new=True,is_select=False)
	__slots__=_defaults.keys()+['colidx', '_property_cache', '_result']
	_init_tuple=('result',)
	def __init__(self, *args, **kwargs):
		for k,v in dict(self._defaults, **kwargs).iteritems(): setattr(self, k, v)
		for idx,v in enumerate(args): setattr(self, self._init_tuple[idx], v)
	@cached_property
	def cols(self): return []
	@cached_property
	def coltypes(self): return {}
	def where_cond(self,row,db):
		if self.has_oids:
			oid=self.store[row][0]
			if oid is None: raise NoKeysError,"None in OID column"
			return dict(OID=int(oid))
		else:
			if not db[self.table].keys: raise NoKeysError,"Table %r has no keys"%(self.table)
			ret={}
			for key in db[self.table].keys:
				val=self.store[row][self.colidx[key]]
				if val is not None and isinstance(val,(str,unicode)) and val.isdigit(): val=int(val)
				ret[key]=val
			return ret
	def conv_dbval2gtkval(self, val):
		if isinstance(val,buffer): val=str(val)
		elif isinstance(val,datetime.datetime):
			val=val.strftime("%c")
		if isinstance(val,str):
			try: val=val.decode("utf8")
			except UnicodeDecodeError:
				val=val.encode("string_escape")
		return val
	@property
	def result(self): return self._result
	@result.setter
	def result(self, result):
		self.sql=result.sql
		self.cols=result.cols
		try: self.table=result.table
		except AttributeError: pass
		if self.cols:
			self.colidx=dict([(x,idx) for idx,x in enumerate(self.cols)])
			self.is_select=True
			self.store=gtk.ListStore(*([object for x in self.cols]+[str]))
			for row in result:
				self.store.append(map(self.conv_dbval2gtkval,row)+[""])
			match=self.search_re.match(self.sql)
			if match:
				if match.group('table') is not None: self.table=match.group('table')
				if match.group('oid') is not None: self.has_oids=True
		self._result=result

class UI(object):
	class NiceViewSolver(Connectable):
		delayed_id=None
		def __init__(self,tbl,db,hbox):
			self.tbl=tbl
			self.db=db
			self.hbox=hbox
			self.reset()
		def reset(self,name=None,sql=None):
			self.children=[]
			self.name=name
			for child in self.hbox.get_children():
				self.hbox.remove(child)
			if name:
				self.hbox.pack_start(gtk.Label("%s: "%(name,)), False, False, 0)
				self.hbox.show_all()
			if sql:
				self.sql=sql%self
		def __getitem__(self,key):
			self.create_input(key)
			return self.db.api.p
		@cached_property
		def sql(self):
			return self.sql
		@cached_property
		def args(self):
			return [x.child.get_text() for x in self.children]
		def set_sql(self,sql):
			self.sql=sql
		def set_name(self,name):
			self.hbox.pack_start("%s: "%(name,), False, False, 0)
			self.name=name
		def on_entry_activate(self,entry):
			self.run_handlers("activate")
		def on_delayed_entry_activate(self):
			self.run_handlers("activate")
			return False
		def delayed_entry_activate(self,entry):
			if self.delayed_id:
				gobject.source_remove(self.delayed_id)
			self.delayed_id=gobject.timeout_add(300,self.on_delayed_entry_activate)
		def create_input(self,key):
			lbl=gtk.Label(key.capitalize())
			lbl.set_use_markup(False)
			self.hbox.pack_start(lbl, False, False, 0)
			lbl.show()
			mdl=gtk.ListStore(str,str)
			entry=gtk.ComboBoxEntry(mdl)
			entry.add_attribute(entry.get_cells()[0],"text",1)
			self.children.append(entry)
			self.hbox.pack_start(entry, False, False, 0)
			try: valquery=self.tbl("definition",{"name":key,"type":"valquery"}).scalar
			except	IndexError: pass
			else:
				for value in self.db(valquery):
					mdl.append((value[0]," | ".join(map(str,value))))
			entry.child.connect("activate",self.on_entry_activate)
			entry.child.connect("changed",self.delayed_entry_activate)
			entry.show()
	def __getattr__(self,key):
		msg='%s.%s has no %r attribute'%(self.__class__.__module__,self.__class__.__name__,key)
		print msg
		raise AttributeError,msg


	def __init__(self,fname=''):
		uipath=os.path.join(os.path.dirname(sys.argv[0]),'db_view.ui')
		try: uipath=unicode(uipath,locale.getdefaultlocale()[1])
		except Exception: pass
		self.ui=GtkBuilderHelper(uipath,self)

		try: self.ui.mainwindow.set_icon_from_file(os.path.join(mypath,'db_view.svg'))
		except gobject.GError,e:
			pass

		self.cur_st=StatementInfo()
		self.in_runquery=False
		self.addbox_items={}

		self.saved_sql_store=gtk.ListStore(str,str)
		self.ui.saved_sql_entry.set_model(self.saved_sql_store)
		self.ui.saved_sql_entry.set_entry_text_column(0)

		self.selection=Selection()
		self.addbox_active_tables={}

		self.dbapimodel=gtk.ListStore(str)
		self.ui.dbapicombo.set_model(self.dbapimodel)
		cell=gtk.CellRendererText()
		self.ui.dbapicombo.pack_start(cell, False)
		self.ui.dbapicombo.add_attribute(cell,'text',0)

		for api in sqllib.DBConn.api_list:
			self.dbapimodel.append((api[0],))
		self.ui.dbapicombo.set_active(0)

		self.sql_select_history=gtk.ListStore(str,str)
		self.ui.select_combo.set_model(self.sql_select_history)
		cell=gtk.CellRendererText()
		self.ui.select_combo.pack_start(cell, False)
		self.ui.select_combo.add_attribute(cell,'text',1)

		self.clipboard=gtk.Clipboard.get(Gdk.atom_intern_static_string("CLIPBOARD"))
		self.clipboard2=gtk.Clipboard.get(Gdk.atom_intern_static_string("PRIMARY"))

		self.sql_history=gtk.ListStore(str)
		try:
			for line in open(os.path.expanduser(os.path.join('~','.dbview_history'))).readlines():
				self.sql_history.append((line.strip(),))
		except IOError: pass

		completion=gtk.EntryCompletion()
		self.ui.sqlquery.set_completion(completion)
		completion.set_model(self.sql_history)
		completion.set_text_column(0)

		self.xref_lbl,self.xref_menu=self.repack_menulbl(self.ui.xref)
		self.xtbl_lbl,self.xtbl_menu=self.repack_menulbl(self.ui.xtbl)

		self.ui.dataview.connect_after('realize',self.on_dataview_realize)
		self.ui.dataview.get_selection().set_mode(gtk.SelectionMode.MULTIPLE)

		self.ui.dataview.set_search_equal_func(self.on_search, None)

		self.tablestore=gtk.ListStore(str,str)
		self.ui.tablesview.set_model(self.tablestore)
		self.ui.tablesview.insert_column(gtk.TreeViewColumn('Name',gtk.CellRendererText(),text=0), -1)
		self.ui.tablesview.insert_column(gtk.TreeViewColumn('Rows',gtk.CellRendererText(),text=1), -1)
		ff=gtk.FileFilter()
		ff.set_name('SQLite 3')
		ff.add_pattern('*.sq3')
		ff.add_pattern('*.sqlite3')
		ff.add_pattern('*.sqlite')
		ff.add_mime_type('application/x-sqlite3')
		self.ui.fchooser.add_filter(ff)
		ff=gtk.FileFilter()
		ff.set_name('All')
		ff.add_pattern('*')
		self.ui.fchooser.add_filter(ff)
		self.load_db(fname)

	def select_api(self,name):
		for idx,api in enumerate(sqllib.DBConn.api_list):
			if name==api[0]: 
				self.ui.dbapicombo.set_active(idx)
				return

	def on_search(self,store,x,user_input,row_iter, user_data):
		user_input=user_input.lower()
		for colval in map(lambda x: store.get_value(row_iter,x),range(store.get_n_columns())):
			if colval is None: continue
			if not isinstance(colval,(str,unicode)): colval=str(colval)
			if user_input in colval.lower(): return False
		return True

	def repack_menulbl(self,menuitem):
		hbox=gtk.HBox()
		menu_child=menuitem.get_child()
		menuitem.remove(menu_child)
		hbox.pack_start(menu_child, False, False, 0)
		menuitem.add(hbox)
		hbox.show()
		lbl=gtk.Label()
		hbox.pack_start(lbl, False, False, 0)
		lbl.show()
		menu=gtk.Menu()
		menuitem.set_submenu(menu)
		return lbl,menu
		
	def on_refresh(self,btn):
		self.refresh_view()
	def	on_expand_btn(self,btn):
		self.refresh_view()

	def on_col_clicked(self,col,idx):
		sql=self.cur_st.sql
		ord_st=' order by '
		limit_st=" limit "
		try: limit_idx=sql.lower().rindex(limit_st)
		except ValueError: limit_st=""
		else:
			limit_st=sql[limit_idx:]
			sql=sql[:limit_idx]
		cur_col=self.cur_st.cols[idx]
		ord_idx=sql.lower().rfind(ord_st)
		if ord_idx==-1: sql=sql+ord_st+cur_col+limit_st
		else:
			after_ord=sql[ord_idx+len(ord_st):].split()
			if after_ord[0]==cur_col:
				if len(after_ord)>1 and after_ord[1].lower()=='desc': after_ord.pop(1)
				else: after_ord.insert(1,'desc')
			after_ord[0]=ord_st+cur_col
			sql=(sql[:ord_idx]+' '.join(after_ord))+limit_st
		self.ui.sqlquery.set_text(sql)
		self.ui.sqlquery.activate()

	def configure_dataview(self,stinfo=None):
		if stinfo==None: stinfo=StatementInfo()
		stinfo.is_new=(stinfo.cols!=self.cur_st.cols)
		self.cur_st=stinfo
		tree_pos=self.ui.dataview.get_visible_rect()
		if stinfo.is_new:
			for col in self.ui.dataview.get_columns(): 
				self.ui.dataview.remove_column(col)
		if stinfo.sql is None:
			self.ui.dataview.set_model(None)
			self.sql_select_history.clear()
		else:
			self.ui.query_lbl.set_text(str(len(stinfo.store)))
			self.ui.select_combo.set_active_iter(self.add_ifnot(self.sql_select_history,stinfo.sql,80))
			self.expand_columns_checked=not self.ui.expand_btn.get_active()
			if stinfo.is_new:
				for idx,title in enumerate(stinfo.cols):
					col=gtk.TreeViewColumn(title.replace('_','__'))
					cell=gtk.CellRendererText()
					col.pack_start(cell, False)
					col.set_resizable(True)
					col.set_clickable(True)
					col.set_cell_data_func(cell,self.render_data,idx)
					col.connect('clicked',self.on_col_clicked,idx)
					self.ui.dataview.append_column(col)
			self.ui.dataview.set_model(stinfo.store)
			self.ui.dataview.columns_autosize()
			if not stinfo.is_new:
				self.ui.dataview.scroll_to_point(tree_pos.x,tree_pos.y)
	def render_data(self,column,cell,model,iter,idx):
		value=model.get_value(iter,idx)
		if value is not None:
			val_str=value.encode("utf8") if isinstance(value, unicode) else str(value)
			valstr_len=len(val_str)
			if self.expand_columns_checked and (valstr_len>30 or '\n' in val_str):
				cell.set_property('text', val_str.replace("\n","\\n").replace("\r","\\r"))
				if cell.get_property("width-chars")<min(30,valstr_len): cell.set_property("width-chars",min(30,valstr_len))
			else:
				cell.set_property('text',val_str)
		else:
			cell.set_property('markup','<span foreground="#808080" size="smaller" style="italic">null</span>')
	def on_dataview_query_tooltip(self,treeview,x,y,kbd_mode,tooltip):
		wx,wy=treeview.convert_widget_to_bin_window_coords(x,y)
		path_info=treeview.get_path_at_pos(wx,wy)
		if path_info is None: return False
		path,column,cell_x,cell_y=path_info
		for idx,col in enumerate(treeview.get_columns()):
			if column==col:
				model=treeview.get_model()
				val=model.get_value(model.get_iter(path),idx)
				if val==None: return
				if isinstance(val,unicode): val=val.encode("utf8")
				else: val=str(val)
				if "\0" in val: val=val.encode("string_escape")
				try: tooltip.set_text(val)
				except Exception,e:
					print "cannot set as tooltip: %s: %r"%(e,val,type(val))
				return True
	def addinfo_update(self,obj):
		cols=[]
		vals=[]
		skip_oid=self.cur_st.has_oids
		for col in self.cur_st.cols:
			if skip_oid:
				skip_oid=False
				continue
			tgl=self.addbox_items[col]['toggle']
			e=self.addbox_items[col]['entry']
			val=self.addbox_items[col]['text']
			if (e==obj and val!=''):
				tgl.set_active(True)
			if tgl.get_active():
				e.show()
				cols.append(col)
				if not isinstance(val, (str, unicode)) or not (val.isdigit() or val.upper()=='NULL'): val=self.dbconn.api.escape(val)
				vals.append(val)

		self.ui.sqlquery.set_text('insert into %s (%s) values (%s)'%(self.cur_st.table,','.join(cols),','.join(vals)))

	def on_addbtn_toggled(self,*args):
		skip_oid=self.cur_st.has_oids
		if not self.ui.addbtn.get_active():
			self.ui.addbox.hide()
			return
		if self.cur_st.is_new:
			previously_set=dict([(k,v['text']) for k,v in self.addbox_items.items() if v['toggle'].get_active()])
			self.addbox_items={}
			for child in self.ui.addbox.get_children():
				self.ui.addbox.remove(child)
			for col in self.cur_st.cols:
				if skip_oid:
					skip_oid=False
					continue
				vbox=gtk.VBox()
				tgl=gtk.CheckButton(col,use_underline=False)
				if col in previously_set: tgl.set_active(True)
				tgl.connect('toggled',self.addinfo_update)
				vbox.show()
				tgl.show()
				vbox.pack_start(tgl, False, False, 0)
				e=gtk.Entry()
				if col in previously_set:
					e.set_text(previously_set[col])
					e.show()
				e.connect('changed',self.on_addentry_changed,col)
				vbox.pack_start(e, True, False, 0)
				self.addbox_items[col]=dict(toggle=tgl,entry=e,container=vbox)
				self.ui.addbox.pack_start(vbox, False, False, 0)
			self.cur_st.is_new=False
		for col,v in self.addbox_items.items(): v['text']=v['entry'].get_text()
		self.ui.addbox.show()

	def on_addentry_changed(self,entry,col):
		self.addbox_items[col]['text']=entry.get_text()
		self.addinfo_update(entry)

	def on_dbapicombo_changed(self,combo):
		self.selected_api=sqllib.DBConn.api_list[combo.get_active()][1]
		if self.selected_api.filename_pat:
			self.ui.fchooser.set_property('visible',True)
		else:
			self.ui.fchooser.set_property('visible',False)

	def on_dataview_row_activated(self,treeview,path,column):
		if not self.ui.raw_btn.get_active(): return
		try: where_cond=self.cur_st.where_cond(path,self.dbconn)
		except NoKeysError,e: return
		tbl=self.cur_st.table
		colname=column.get_title().replace('__','_')
		val=self.dbconn.api.escape(self.dbconn.scalar(tbl,colname,dict([(k,sqllib.Eq(v,self.dbconn.api.p)) for k,v in where_cond.iteritems()])))
		try: val=val.decode("utf8")
		except UnicodeDecodeError: pass
		condstr=' where '+' AND '.join(['%s=%s'%(k,self.dbconn.api.escape(v)) for k,v in where_cond.iteritems()])
		s1='update %s set %s='%(self.cur_st.table,colname)
		self.ui.sqlquery.set_text(s1+val+condstr)
		self.ui.sqlquery.grab_focus()
		self.ui.sqlquery.select_region(len(s1)+[0,1][val[0]=="'"],len(s1)+len(val)-[0,1][val[-1]=="'"])

	def on_clearquery(self,btn):
		self.ui.sqlquery.set_text('')

	def add_ifnot(self,store,txt,maxlen=None):
		for n,row in enumerate(store):
			if row[0]==txt:
				store.move_after(store.get_iter((n,)),None)
				return store.get_iter((0,))
		if maxlen is not None:
			if len(txt)>maxlen:
				s=maxlen/2-2
				return store.insert(0,(txt,'%s ... %s'%(txt[:s],txt[-s:])))
			else: return store.insert(0,(txt,txt))
		else: return store.insert(0,(txt,))

	def set_error(self,err=None):
		if err is None:
			self.ui.errorbox.set_property('visible',False)
		else:
			self.ui.error_label.set_text(str(err)) 
			self.ui.errorbox.set_property('visible',True)

	def on_select_combo_changed(self,combobox):
		if not self.in_runquery:
			self.run_query(combobox.get_model()[combobox.get_active()][0])

	def create_csvdata(self,delim="\t"):
		copy_text=[]
		for row in self.cur_st.store:
			copy_row=[]
			for cell in row:
				if cell is None: cell=""
				if not isinstance(cell,(str,unicode)): cell=str(cell)
				if '\n' in cell or '\r' in cell or '"' in cell or delim in cell: cell='"%s"'%(cell.replace('"','""').replace("\r\n","\n"))
				copy_row.append(cell)
			copy_text.append(delim.join(copy_row))
		return '\n'.join(copy_text)

	def on_copy_all(self,menuitem):
		copy_text=self.create_csvdata()
		self.clipboard.set_text(copy_text, -1)
		self.clipboard2.set_text(copy_text, -1)

	def on_save_all_activate(self,menuitem):
		chooser=gtk.FileChooserDialog(action=gtk.FILE_CHOOSER_ACTION_SAVE,buttons=(gtk.STOCK_SAVE,1))
		chooser.set_default_response(1)
		resp=chooser.run()
		filename=chooser.get_filename()
		chooser.destroy()
		if resp==1:
			print "Save to: %r"%(filename,)
			copy_text=self.create_csvdata(",")
			open(filename,"w").write(copy_text)

	def on_dataview_event_after(self,treeview,ev):
		if ev.type==Gdk.EventType.BUTTON_PRESS and ev.button.button==3:
			self.selection.update_selection(treeview,ev)
			if self.selection:
				self.xref_lbl.set_text(short_str(self.selection.text))
				self.xtbl_lbl.set_text(self.selection.col)

				self.ui.menu_copy.set_sensitive(True)
				self.ui.xref.set_sensitive(True)
				self.ui.xadd.set_sensitive(True)
			else:
				self.ui.menu_copy.set_sensitive(False)
				self.ui.xref.set_sensitive(False)
				self.ui.xadd.set_sensitive(False)

			oids_active=(self.cur_st.has_oids and len(self.selection.rows)>0)
			self.ui.menu_del.set_sensitive(oids_active)
			self.ui.menu_apply.set_sensitive(oids_active)
			self.ui.dv_menu.popup(None, None, None, None, ev.button.button, ev.time)

	def on_dataview_realize(self,*args):
		print 'on_dataview_realize:',args

	def on_menu_copy(self,menuitem):
		if self.selection:
			self.clipboard.set_text(' '.join(map(str,self.selection.get_cross_select())), -1)
			self.clipboard2.set_text(' '.join(map(str,self.selection.get_cross_select())), -1)
	def on_menu_del(self,menuitem):
		if self.selection.rows:
			self.ui.sqlquery.set_text('delete from %s where OID in (%s)'%(self.cur_st.table,','.join(self.selection.get_cross_select(0))))

	def on_menu_apply(self,menuitem):
		if self.selection.rows:
			self.ui.sqlquery.set_text('__ %s where OID in (%s)'%(self.cur_st.table,','.join(self.selection.get_cross_select(0))))
			

	def run_query(self,sql,args=()):
		if self.in_runquery:
			print "double run_query: %s"%(sql)
			traceback.print_stack()
			return
		self.in_runquery=True
		try: result=self.dbconn(sql,*args)
		except Exception,e: self.set_error(e)
		else:
			self.set_error(None)
			self.add_ifnot(self.sql_history,sql)
			self.stinfo=StatementInfo(result)
			if not self.stinfo.is_select: self.refresh_tablelist()
			else:
				try: idx=sql.lower().index('from')
				except ValueError: 
					self.current_select_from=None
				else:
					self.current_select_from=sql[idx:]
				self.configure_dataview(self.stinfo)
		self.in_runquery=False

	def on_sqlquery(self,entry):
		self.set_error(None)
		args=()
		if not self.ui.raw_btn.get_active():
			args=self.nvresolver.get_args()
		self.run_query(entry.get_text(),args)

	def on_choose_table(self,treeview,path,column):
		if self.ui.limitbtn.get_active(): limitstr=' limit 500'
		else: limitstr=''
		tblname=self.tablestore[path][0]
		if '-' in tblname: tblname='`%s`'%tblname
		sql='select %s* from %s%s'%(self.dbconn.api.oidstr,tblname,limitstr)
		self.ui.sqlquery.set_text(sql)
		self.ui.sqlquery.activate()
		self.on_addbtn_toggled()

	def on_sqlenter(self,btn):
		self.ui.sqlquery.activate()

	def on_reload_select(self,btn):
		self.ui.sqlquery.set_text(self.cur_st.sql)

	def on_save_sql_clicked(self,btn):
		raise NotImplementedError,"Need to update"
		name=self.ui.saved_sql_entry.child.get_text()
		sql=self.ui.sqlquery.get_text()
		try: self.con.execute('insert into saved_sql (name,sql) values (?,?)',[name,sql])
		except sqlapi.IntegrityError:
			self.con.execute('update saved_sql set sql=? where name=?',[sql,name])
		self.refresh_view()

	def on_saved_sql_entry_changed(self,entry):
		active=entry.get_active()
		if active==-1: return		# user changed manually text
		self.ui.sqlquery.set_text(self.saved_sql_store[active][1])

	def refresh_view(self):
		self.refresh_tablelist()
		if self.cur_st.sql is not None:
			self.run_query(self.cur_st.sql)

	lbl_idx_list=['%d'%x for x in range(1,10)]+['0']+['%c'%(x) for x in range(ord('a'),ord('z')+1)]
	def lbl_idx(self,idx,text):
		try: return '_%s %s'%(self.lbl_idx_list[idx],text.replace('_','__'))
		except IndexError:
			return text.replace('_','__')

	def add_menu_item(self,menu,label,callback,*cbargs):
			mi=gtk.MenuItem(label=label,use_underline=True)
			menu.append(mi)
			mi.show()
			mi.connect('activate',callback,*cbargs)

	def refresh_tablelist(self):
		self.tablestore.clear()
		for child in self.xref_menu.get_children(): self.xref_menu.remove(child)
		for child in self.xtbl_menu.get_children(): self.xtbl_menu.remove(child)
		self.table_list=[]
		for idx,name in enumerate(self.dbconn.api.table_names()):
			count=None
			if "NO_DBCOUNT" not in os.environ:
				try: pg_count=self.dbconn["pg_class"]('reltuples', {"relname":name}).scalar
				except: pg_count=None
				if pg_count is None or pg_count<int(os.environ.get("PG_MAX_COUNT","100000")):
					try: count=self.dbconn.scalar(name,'count(*)')
					except:
						print >>sys.stderr,"Failed loading count(*) for %s"%(name)
				else: count=u"%s\xb1\u03b1"%(int(pg_count),)
			self.tablestore.append((name, str(count)))
			self.add_menu_item(self.xref_menu,self.lbl_idx(idx,name),self.on_xref_activate,name)
			self.add_menu_item(self.xtbl_menu,self.lbl_idx(idx,name),self.on_xtbl_activate,name)

	def on_xadd_activate(self,menuitem):
		v=self.addbox_items.get(self.selection.col,None)
		if v is not None:
			v['text']=self.selection.text
			v['entry'].set_text(v['text'])
			v['toggle'].set_active(True)

	def on_xref_activate(self,menuitem,tblname):
		sel=self.selection.text 
		if sel is None: cond=' is null'
		elif isinstance(sel, (str,unicode)) and sel.isdigit(): cond='=%s'%(sel,)
		else: cond='=%s'%(self.dbconn.api.escape(sel))
		self.ui.sqlquery.set_text('select %s* from %s where %s%s'%(self.dbconn.api.oidstr,tblname,self.selection.col,cond))
		self.ui.sqlquery.activate()

	def on_xtbl_activate(self,menuitem,tblname):
		self.ui.sqlquery.set_text('select %s* from %s where %s in (select %s %s)'%(self.dbconn.api.oidstr,tblname,self.selection.col,self.selection.col,self.current_select_from.replace(' limit 500','')))
		self.ui.sqlquery.activate()

	def on_dbnameentry_activate(self,entry):
		name=entry.get_text()
		if name:
			self.load_db(name)
	def get_password(self, msg):
		dlg=gtk.Dialog(title=msg, buttons=(gtk.STOCK_OK, gtk.RESPONSE_ACCEPT))
		e=gtk.Entry()
		e.set_visibility(False)
		dlg.get_action_area().add(e)
		e.show()
		dlg.run()
		ret=e.get_text()
		dlg.destroy()
		return ret
	@property
	def current_dbname(self): return self.ui.dbnameentry.get_text()
	@current_dbname.setter
	def current_dbname(self, v): self.ui.dbnameentry.set_text(v)
	def load_db(self,dbname):
		self.current_dbname=dbname
		try:
			try: self.dbconn=sqllib.DBConn(dbname,self.selected_api)
			except Exception,e:
				if "password" in str(e):
					self.dbconn=sqllib.DBConn(dbname+" password="+self.get_password(str(e)),self.selected_api)
				else: raise
		except Exception,e:
			print 'Error loading %r'%(dbname)
			self.set_error(e)
			self.tablestore.clear()
		else:
			self.set_error(None)
			if self.selected_api.filename_pat:
				fname=self.ui.fchooser.get_filename()
				if self.selected_api.filename_pat%fname!=dbname:
					self.ui.fchooser.set_filename(os.path.abspath(dbname))

			try: dbview_table=self.dbconn.db_view
			except Exception: self.renice_dbview(None)
			else: self.renice_dbview(dbview_table)

			self.configure_dataview()
			self.refresh_view()

	def set_raw_view(self,is_raw):
		raw_visible=dict(sql_hbox=True,tables_scwin=True,views_scwin=False,view_conf=False,reload_select=True)
		for elem,elem_raw in map(lambda x: (getattr(self.ui,x[0]),x[1]),raw_visible.items()):
			if (is_raw and elem_raw) or ((not is_raw) and (not elem_raw)): elem.show()
			else: elem.hide()
		if not self.nicetbl: return
		self.nvresolver.reset()
		self.ui.defviews_store.clear()
		for name,definition in self.nicetbl("name,definition","type='view'"):
			self.ui.defviews_store.append((name,definition))
	def run_custom_query(self):
		self.run_query(self.nvresolver.get_sql(),self.nvresolver.get_args())
	def on_defview(self,view,path,column):
		mdl=view.get_model()
		i=mdl.get_iter(path)
		self.nvresolver.reset(mdl.get_value(i,0),mdl.get_value(i,1))
		self.run_custom_query()
	def renice_dbview(self,tbl):
		self.nicetbl=tbl
		if tbl is None:
			self.set_raw_view(True)
			self.ui.raw_btn.hide()
			return
		self.nvresolver=self.NiceViewSolver(tbl,self.dbconn,self.ui.view_conf)
		self.nvresolver.connect("activate",self.on_nvr_activate)
		self.ui.raw_btn.show()
		self.ui.raw_btn.set_active(False)
	def on_nvr_activate(self,nvr):
		self.run_custom_query()
	def on_raw_btn(self,btn):
		self.set_raw_view(btn.get_active())
	def on_fchooser(self,fchooser):
		fname=fchooser.get_filename()
		if type(fname)==str and not (fname==self.current_dbname) and os.path.isfile(fname):
			self.load_db(fname)
			self.ui.mainwindow.set_title(os.path.basename(fname))
	def on_quit(self,*args):
		gtk.main_quit()
	def run(self,fname=None):
		if fname is not None: self.load_db(fname)
		gtk.main()
		if len(self.sql_history)>0:
			try: open(os.path.expanduser(os.path.join('~','.dbview_history')),'w').write(''.join(['%s\n'%x[0] for x in self.sql_history][:2000]))
			except IOError: pass

if __name__=='__main__':
	ui=UI()
	fname=None
	if len(sys.argv)>1: fname=sys.argv[1]
	if len(sys.argv)>2: ui.select_api(sys.argv[2])
	ui.run(fname)
