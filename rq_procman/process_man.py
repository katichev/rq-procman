import datetime
import multiprocessing
import os, signal
from process import Process
from Queue import Empty
from uuid import uuid4

MAX_WAIT = 10

class ProcRunner(object):
    def __init__(self, cmd_pipe):
        self.cmd_pipe = cmd_pipe  # command pipe
        self.events = multiprocessing.Queue()  # queue to handle process statuses

        self._proc_not_started = {}
        self._proc_started = {}
        self._proc_in_progress = {}
        self._proc_finished = {}

        self._terminating = False
        self._killed = False
        
    def run(self):
        """
            Main loop of ProcRunner
        """
        self.cmd_pipe.send('started')
        
        while True:
            try:    
                if self.cmd_pipe.poll():
                    self._parse_cmd(self.cmd_pipe.recv())
            except EOFError:
                self._terminating = True

            try:
                msg = self.events.get(timeout=0.1)
            except Empty:
                pass
            else:
                name, mtype, data = msg 
                if mtype == 'start':
                    p = self._proc_started.pop(name)
                    p['pid'] = data
                    self._proc_in_progress[name] = p

                elif mtype == 'stop':
                    p = self._proc_in_progress.pop(name)
                    p['returncode'] = data
                    self._proc_finished[name] = p

            
            if not self._terminating and self._proc_not_started:
                new_names = self._proc_not_started.keys()
                for name in new_names:
                    p = self._proc_not_started.pop(name)
                    p['process'] = multiprocessing.Process(name=name,
                                                           target=p['obj'].run,
                                                           args=(self.events,))
                    p['process'].start()
                    self._proc_started[name] = p

            if self._terminating:
                if not self._proc_in_progress and not self._proc_started:
                    break

                waiting = datetime.datetime.now() - self._exit_start
                if not self._killed and waiting > datetime.timedelta(seconds=MAX_WAIT):
                    self._proc_started = {} # consider them lost
                    self._killall(force=True)            
                    self._killed = True 

                    
    def _killall(self, force=False):
        """Kill all remaining processes, forcefully if requested."""
        for_termination = [name for name,p in self._proc_in_progress.items() if p.get('pid',None)]
        for n in for_termination:
            p = self._proc_in_progress[n]
            if force:
                self._kill(p['pid'])
            else:
                self._term(p['pid'])

    def _kill(self, pid):
        try:
            os.killpg(pid, signal.SIGKILL)
        except OSError as e:
                if e.errno not in [errno.EPERM, errno.ESRCH]:
                    raise

    def _term(self, pid):
        try:
            os.killpg(pid, signal.SIGTERM)
        except OSError as e:
                if e.errno not in [errno.EPERM, errno.ESRCH]:
                    raise
    
    def cmd_term(self, cmd):
        if self._terminating:
            return
        self._terminating = True
        print "Terminating..."
        if self._proc_in_progress or self._proc_started:
            self._exit_start = datetime.datetime.now()
            self._killall()

    def _parse_cmd(self, cmd):
        if cmd['action']=='add':
            self.cmd_add_process(cmd)
        if cmd['action']=='term':
            self.cmd_term(cmd)
        if cmd['action']=='stat':
            self.cmd_get_status(cmd)

    def cmd_add_process(self, cmd):
        if self._terminating:
            return
            
        name = cmd.get('name', None)
        env = cmd.get('env', None)
 
        if not name:
            name = str(uuid4())
        
        proc = Process(cmd['cmd'],
                name=name,
                env=env)

        self._proc_not_started[name] = {}
        self._proc_not_started[name]['obj'] = proc
        
        self.cmd_pipe.send({'name':name, 'status': 'queued'})        
        
    def cmd_get_status(self,cmd):
        name = cmd.get('name', None)
        status = 'not found'
        if name in self._proc_not_started:
            status = 'queued'
        elif name in self._proc_in_progress:
            status = 'started, pid %s'%self._proc_in_progress[name]['pid']
        elif name in self._proc_started:
            status = 'pending'
        elif name in self._proc_finished:
            status = 'finished, returncode: %s'%self._proc_finished[name]['returncode'] 

        self.cmd_pipe.send({'name':name, 'status':status})

        
class ProcessMan(object):
    """
    Process Manager
    """
    def __init__(self):
        self.commands_pipe, child = multiprocessing.Pipe()
        self._proc_runner = ProcRunner(child)

        self._is_running = False

    def start(self):
        self._proc = multiprocessing.Process(name='runner',
                                            target=self._proc_runner.run,
                                            )
        self._proc.start()
        res = self.commands_pipe.recv()
        if res!='started':
            raise Exception('Not Started!')

        self._is_running = True

    def terminate(self):
        if not self._is_running:
            return
        self.commands_pipe.send({'action':'term'})

    def add_process(self, cmd):
        if not self._is_running:
            return
        self.commands_pipe.send({'action':'add', 'cmd':cmd})   
        res = self.commands_pipe.recv()
        print res

    def status(self,name):
        if not self._is_running:
            return
        self.commands_pipe.send({'action':'stat', 'name':name})
        res = self.commands_pipe.recv()
        print res
    

def main():
    from time import sleep
    lpm = ProcessMan()
   
    e1 = {'PYTHONUNBUFFERED':	'1',}
    
    e = os.environ.copy()
    e.update(e1)
   
    lpm.start()
    print lpm.add_process('echo 123')
    lpm.add_process('man ssh')

    sleep(10)
    lpm.terminate()
    
    
if __name__=="__main__":
    main()
