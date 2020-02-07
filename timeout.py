

import multiprocessing


class Timeout:
    def __init__(self, seconds):
        self.pipe = Pipe(False)
        self.proc = None
        self.sec = seconds
    
    # for "with Distribute() as d:"
    def __enter__(self):
        
    @staticmethod
    def thread_wrap(target_func, pipe, args, kwargs):
        return target_func(*args, **kwargs)
        
    def run(self, target_func, *args, **kwargs):
        self.proc = multiprocessing.Process(target=self.thread_wrap, 
                    args=(target_func, self.pipes[i][1], args, kwargs))
        self.proc.start()
    
        # wait for it to finish while waiting to get pipe
        self.proc.join(self.sec)
        
        if p.is_alive():
            self.kill_proc()
            raise Exception('Thread is still running.')

    # If thread is still active
    if p.is_alive():
        print "running... let's kill it..."

        # Terminate
        p.terminate()
        p.join()
        
        self.p.start()
        return self
    
    p.join(10)
    
    
    
    
    def __exit__(self, type, value, traceback):
        self.kill_proc()
        
    def kill_proc(self):
        # close explicitly
        if self.proc is not None:
            self.proc.terminate()
            self.proc.join()





    p = multiprocessing.Process(target=bar)
    p.start()

    # Wait for 10 seconds or until process finishes
    p.join(10)

    # If thread is still active
    if p.is_alive():
        print "running... let's kill it..."

        # Terminate
        p.terminate()
        p.join()
        

