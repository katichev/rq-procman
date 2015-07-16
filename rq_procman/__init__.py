from .process_man import ProcessMan
from rq import Worker

class PMWorker(Worker):
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)
        self.pm = None        

    def work(self, burst=False):
        """Starts the work loop 
        """
        setup_loghandlers()
        self._install_signal_handlers()

        self.pm = ProcessManager()
        did_perform_work = False
        self.register_birth()
        self.log.info("RQ worker {0!r} started, version {1}".format(self.key, VERSION))
        self.set_state(WorkerStatus.STARTED)

        try:
            while True:
                try:
                    if not burst or not self.pm.has_running_jobs:
                        # we don't want to quit in burst mode if there are some task in progress
                        # TODO: doe it make sense to have a burst flag for this kind of worker?
                        self.check_for_suspension(burst)

                    if self.should_run_maintenance_tasks:
                        # take care about timed out jobs
                        # determine if they belongs to this worker first, check status somehow and then kill
                        # TODO: Do we need a separate queues here? is it possible that other worker may accidently
                        # wipe StartedJobRegistry?
                        self.clean_registries()

                    if self._stop_requested:
                        # flag is set by a signal
                        self.log.info('Stopping on request')
                        break

                    timeout = None if burst else max(1, self.default_worker_ttl - 60)
                    # None means 'non-blocking' here
                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        if burst:
                            self.log.info("RQ worker {0!r} done, quitting".format(self.key))
                        break
                except StopRequested:
                    # raised somewhere by signal
                    break

                job, queue = result
                self.execute_job(job)
                self.heartbeat()

                if job.get_status() == JobStatus.FINISHED:
                    queue.enqueue_dependents(job)

                did_perform_work = True

        finally:
            # stop all jobs since we are 
            if not self.is_horse:
                self.register_death()
        return did_perform_work

    def execute_job(self, job):
        """Execute job in same thread/process, do not fork()"""
        return self.perform_job(job)
        
    def perform_job(self, job):
        # update job status, move to appropriate registry
        self.prepare_job_execution(job)

        self.pm.put_job(job)




    def main_work_horse(self, *args, **kwargs):
        raise NotImplementedError("PMWorker does not implement this method")


