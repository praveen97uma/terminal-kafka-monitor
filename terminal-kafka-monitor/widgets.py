import npyscreen
import psutil
import sys


class MultiLineWidget(npyscreen.BoxTitle):
    '''
        A framed widget containing multiline text
    '''
    _contained_widget = npyscreen.MultiLineEdit

class CustomMultiLineAction(npyscreen.MultiLineAction):
    '''
        Making custom MultiLineAction by adding the handlers
    '''
    def __init__(self,*args,**kwargs):
        super(CustomMultiLineAction,self).__init__(*args,**kwargs)
        self.add_handlers({
            "^K": self.kill_process,
            "q" : self.quit
        })

    def kill_process(self,*args,**kwargs):
        pid = self.values[self.cursor_line].split()[1]
        target = psutil.Process(int(pid))
        target.terminate()

    def quit(self,*args,**kwargs):
        sys.exit()


class MultiLineActionWidget(npyscreen.BoxTitle):
    '''
        A framed widget containing multiline text
    '''
    _contained_widget = CustomMultiLineAction

class MonitorWindowForm(npyscreen.FormBaseNew):
    def create(self, *args, **kwargs):
        super(MonitorWindowForm, self).create(*args, **kwargs)

    def while_waiting(self):
        pass


