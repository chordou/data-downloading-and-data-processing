import os
import win32com.client
filename_macro =r'C:\Users\QIUCHUCHU428\PycharmProjects\test\FX\macro_for_refresh_1029.xlsm'
if os.path.exists(filename_macro):
    xl = win32com.client.Dispatch('Excel.Application')
    xl.Workbooks.Open(Filename = filename_macro, ReadOnly=1)
    xl.Application.Run("UpdateTickerAndRefresh_fx")
    xl.Application.Quit()
    del xl
#PRINT FINAL COMPLETED MESSAGE#
print("Macro refresh completed!")