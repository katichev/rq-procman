## http://pymotw.com/2/subprocess/
## https://docs.python.org/2/library/subprocess.html    
import os
import signal
import subprocess
from reprint import RePrint

class Process(object):
    def __init__(self, command, name=None, env=None):
        self.env = os.environ.copy() if env is None else env
        self.name=name
        self.command = command

    def run(self, events=None):
        rp = RePrint(self.name)
        child = subprocess.Popen(self.command, 
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        bufsize=1,  #line buffering
                        shell=True,
                        close_fds=True,
                        preexec_fn=os.setsid, # to be able to send signals to childs via killpg
                        env=self.env)
        
        events.put((self.name, 'start', child.pid))

        #this process will exit at child's exit
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        
        for line in iter(child.stdout.readline, b''):
            # say to redis
            rp.msg(str(line))
        
        child.stdout.close()
        child.wait()

        events.put((self.name, 'stop', child.returncode)) 
