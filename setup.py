#!/usr/bin/python

from distutils.core import setup
import py2exe
import sys,os
sys.path.append("lib")
sys.path.append(os.path.join("..","lib"))

gtk_includes="cairo pango pangocairo atk gobject gio".split()
base_includes="socket gzip BaseHTTPServer sqlite3.dbapi2".split()
local_includes="krutils.misc krutils.gtkutil krutils.sql".split()

setup(name="db_view",description="Database Viewer",version="0.9.1",
	windows=[dict(script="db_view.py")],
	options=dict(
		py2exe=dict(
			packages='encodings',
			includes=",".join(base_includes+gtk_includes+local_includes),
                        dll_excludes='libglade-2.0-0.dll',
		)
	),
	data_files=['db_view.ui','db_view.svg']
)
