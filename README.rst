===========================================
SQLite (and other SQL) Gtk+ Database viewer
===========================================

Dependencies
============

- Python (2.6+ recommended) - http://python.org/
- Gtk+ - http://www.pygtk.org/
- Python sqlite libraries (usually included with python)
- KrUtils - http://github.com/korc/krutils
    Recommended to install into "lib" directory under parent directory of db_view

Running under Windows
=====================

- Install Python2.7
- Install pygtk all-in-one package
- Install krutils into ..\\lib\\krutils
- Run db_view.py

Creating installer package
--------------------------

- Download all dependencies and make sure db_view.py runs
- Install py2exe (http://www.py2exe.org/)
- Run following in db_view directory::

    setup.py py2exe

- Install InnoSetup (http://www.jrsoftware.org/isdl.php)
- Open db_view.iss with InnoSetup
- Choose [Build] -> [Compile] from menu
- dbview_setup.exe will be created in Output directory under db_view
