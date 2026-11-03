Unicode True
Name "PIDM ${VERSION}"
OutFile "${OUT_FILE}"
InstallDir "$LOCALAPPDATA\Programs\PIDM"
InstallDirRegKey HKCU "Software\PIDM" "InstallLocation"
RequestExecutionLevel user
SetCompressor /SOLID lzma
VIProductVersion "${PRODUCT_VERSION}"
VIAddVersionKey /LANG=1033 "ProductName" "PIDM"
VIAddVersionKey /LANG=1033 "ProductVersion" "${VERSION}"
VIAddVersionKey /LANG=1033 "FileDescription" "PIDM Download Manager Installer"
VIAddVersionKey /LANG=1033 "LegalCopyright" "Saeed Masoudi"

!include "MUI2.nsh"
!define MUI_ABORTWARNING
!define MUI_ICON "${ICON_FILE}"
!define MUI_UNICON "${ICON_FILE}"
!define MUI_FINISHPAGE_RUN "$INSTDIR\PIDM.exe"

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_COMPONENTS
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_LANGUAGE "English"

Section "PIDM" MainSection
  SectionIn RO
  SetShellVarContext current
  DeleteRegValue HKCU "Software\Microsoft\Windows\CurrentVersion\Run" "PIDM"
  RMDir /r "$INSTDIR"
  SetOutPath "$INSTDIR"
  File /r "${SOURCE_DIR}\*"
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  CreateDirectory "$SMPROGRAMS\PIDM"
  CreateShortcut "$SMPROGRAMS\PIDM\PIDM.lnk" "$INSTDIR\PIDM.exe"
  CreateShortcut "$SMPROGRAMS\PIDM\Uninstall PIDM.lnk" "$INSTDIR\Uninstall.exe"
  CreateShortcut "$DESKTOP\PIDM.lnk" "$INSTDIR\PIDM.exe"

  WriteRegStr HKCU "Software\PIDM" "InstallLocation" "$INSTDIR"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "DisplayName" "PIDM"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "DisplayVersion" "${VERSION}"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "DisplayIcon" "$INSTDIR\PIDM.exe"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "Publisher" "Saeed Masoudi"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "URLInfoAbout" "https://github.com/saeedmasoudie/PIDM"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "UninstallString" '"$INSTDIR\Uninstall.exe"'
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "QuietUninstallString" '"$INSTDIR\Uninstall.exe" /S'
  WriteRegDWORD HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "NoModify" 1
  WriteRegDWORD HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM" "NoRepair" 1
SectionEnd

Section /o "Start PIDM with Windows" StartupSection
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Run" "PIDM" '"$INSTDIR\PIDM.exe"'
SectionEnd

Section "Uninstall"
  SetShellVarContext current
  DeleteRegValue HKCU "Software\Microsoft\Windows\CurrentVersion\Run" "PIDM"
  DeleteRegKey HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\PIDM"
  DeleteRegKey HKCU "Software\PIDM"
  Delete "$DESKTOP\PIDM.lnk"
  RMDir /r "$SMPROGRAMS\PIDM"
  RMDir /r "$INSTDIR"
SectionEnd

LangString MainDescription ${LANG_ENGLISH} "Install PIDM, Start menu shortcuts, a desktop shortcut, and an uninstaller."
LangString StartupDescription ${LANG_ENGLISH} "Launch PIDM automatically after you sign in to Windows. This can also be changed later in PIDM settings."

!insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
  !insertmacro MUI_DESCRIPTION_TEXT ${MainSection} $(MainDescription)
  !insertmacro MUI_DESCRIPTION_TEXT ${StartupSection} $(StartupDescription)
!insertmacro MUI_FUNCTION_DESCRIPTION_END
