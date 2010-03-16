[Setup]
AppName=DBView
AppVerName=DBView 0.9
AppPublisher=Cyberdefense Institute Inc.
AppPublisherURL=http://cyberdefense.jp/
DefaultDirName={pf}\DBView
DefaultGroupName=DBView
DisableProgramGroupPage=true
OutputBaseFilename=dbview_setup
Compression=lzma
SolidCompression=true
AllowUNCPath=false
VersionInfoVersion=1.0
VersionInfoCompany=Cyberdefense Institute Inc.
VersionInfoDescription=Database Viewer

[Dirs]
Name: {app}; Flags: uninsalwaysuninstall;

[Files]
Source: dist\*; DestDir: {app}; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: {group}\DBView; Filename: {app}\db_view.exe; WorkingDir: {app}

[Run]
Filename: {app}\db_view.exe; Description: {cm:LaunchProgram,db_view}; Flags: nowait postinstall skipifsilent
