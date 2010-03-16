#!/usr/bin/python

from distutils.core import setup
import py2exe
import sys
sys.path.append("lib")

setup(name="db_view",description="Database Viewer",version="0.9.0",
	windows=[dict(script="db_view.py")],
	options=dict(
		py2exe=dict(
			packages='encodings',
			includes='cairo,pango,pangocairo,atk,gobject,socket,gzip,gtk.glade,BaseHTTPServer,sqlite3.dbapi2,krutils.misc,krutils.gtkutil,krutils.sql',
                        dll_excludes='libglade-2.0-0.dll',
		)
	),
	data_files=['db_view.ui','db_view.svg']
)
