#!/usr/bin/python

from distutils.core import setup
import py2exe
import sys
sys.path.append("lib")

setup(name="db_view",description="Database Viewer",version="0.8.3",
	windows=[dict(script="db_view.py")],
	options=dict(
		py2exe=dict(
			packages='encodings',
			includes='cairo,pango,pangocairo,atk,gobject,socket,gzip,gtk.glade,BaseHTTPServer,pysqlite2.dbapi2,korcutil,gtkutil,sqllib'
		)
	),
	data_files=['db_view.glade','db_view.svg']
)
