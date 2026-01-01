from metaflow import step, Flow, Run, namespace
from metaflow.integrations import ArgoEvent
from obproject import ProjectFlow
from time import sleep
import time
from datetime import datetime

### Map ###
# How to pass parameters in trigger event payload?
    # https://docs.metaflow.org/metaflow/basics#how-to-define-parameters-for-flows
    # https://docs.metaflow.org/production/event-triggering/external-events#advanced-case-publishing-events-inside-a-flow
    # What are we passing?
        # In our toy example: "How to take a parameter set and produce an evaluation result?"
        # Any info or metadata needed to modify the _triggered flow_'s Parameter values. 
# How does task runtime determine run ID result of event trigger and listen for the run result?
    # Time to get a bit creative, how does calling task process listen to and manage the FlowA1 runs? 
    # sleep n minutes, enter loop that polls, trying to locate the run ID triggered by our event?
    # (How) does event publication leave a trace?  
        # FlowB1 sets event_id = ArgoEvent(...).safe_publish(...) and uses to filter when searching for corresponding FlowA1 Run:
        # So the task that calls FlowA1 submits, then loops through latest Runs of target flow_name until one with event_id appears, bingo.
        # Then, the calling task can run another polling loop to monitor that particular Run's status.
            # https://docs.metaflow.org/api/client#run
# Are there complications in x-project event triggering?
    # What is the general relationship between event triggering and projects?
        # Events are scoped by perimeter. Projects are a level below perimeter too, project/branches another. 
            # Projects and Events are siblings in the abstraction hierarchy.
        # Thus, Events dispatched from anywhere will be heard by all listening workflows across all project branches.
        # Here FlowB1 in @project(name="obproject_b") dispatches events that trigger FlowA1
# Basic system design:
    # FlowA1 uses @trigger(event='eval_task_submit')
    # FlowB1 runs ArgoEvent(name='eval_task_submit').safe_publish(...) and waits for results (use cheap @resources!)
        # https://docs.metaflow.org/production/event-triggering/external-events#advanced-case-publishing-events-inside-a-flow
    # FlowA1 uses latest = Parameter("latest", default="2026-01-01")
    # FlowB1 runs ArgoEvent(...).safe_publish(payload={"latest": datetime.now().isoformat()})


def locate_run(flow_name, event_id, event_publish_time, poll_interval=10, timeout=600):
    """Locate a run by event_id across all namespaces (cross-project).

    The event_id from ArgoEvent.safe_publish() matches run.trigger.event.id
    See: https://docs.metaflow.org/production/event-triggering/inspect-events
    """
    # Use global namespace to search across all projects
    original_ns = namespace(None)
    try:
        start_time = time.time()
        while time.time() - start_time < timeout:
            for run in Flow(flow_name).runs():
                if run.created_at < event_publish_time:
                    break  # All remaining runs are older, stop searching
                # Event ID is accessed via run.trigger.event.id
                if run.trigger and run.trigger.event and run.trigger.event.id == event_id:
                    return run
            sleep(poll_interval)
        raise TimeoutError(f"Timeout waiting for run {flow_name} with event ID {event_id}")
    finally:
        namespace(original_ns) # Restore original namespace


def wait_for_successful_run_completion(run_pathspec, poll_interval, timeout=600):
    start_time = time.time()
    while True:
        run = Run(run_pathspec)
        if run.finished and run.successful:
            return run
        sleep(poll_interval)
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timeout waiting for run {run_pathspec} to complete")
    return None

# @project automatically synchronized with obproject.toml when using obproject.ProjectFlow.
# In this example, the notable difference is these are different in /prj-a and /prj-b. 
class FlowB1(ProjectFlow):

    """
    This is a toy flow that shows how to call another flow (A1Flow), cross-project.
    This flow is in project /prj-b, but calls flow A1 from project /prj-a.
    The DAG structure runs two parallel branches of A1Flow, with different params.
    The results are aggregated and then used to do some work.
    """

    @step
    def start(self):
        import random
        # Simulate some process that ingests info and configures a set of parameters to try.
        self.paramset1 = {"param_a": random.randint(1, 100), "param_b": random.randint(1, 10)}
        self.paramset2 = {"param_a": random.randint(1, 100), "param_b": random.randint(1, 10)}
        self.next(self.run_a1_paramset1, self.run_a1_paramset2)

    def _operate_a1_run(self, paramset, poll_interval=10, timeout=300):

        try:
            self.event_id = ArgoEvent(name='eval_task_submit').safe_publish(payload=paramset)
            self.event_publish_time = datetime.now()
        except Exception as e:
            print(f"Error publishing event: {e}")
            return None
        print(f"Event ID: {self.event_id}")
        
        try:
            self.run = locate_run(flow_name="FlowA1", event_id=self.event_id, event_publish_time=self.event_publish_time)
        except Exception as e:
            print(f"Error locating run: {e}")
            return None
        
        trigger = self.run.trigger
        if trigger and trigger.event:
            print(f"Found run: {self.run.id} triggered by {trigger.event.name} ({trigger.event.id} | {trigger.event.timestamp} | {trigger.event.type})")
        else:
            print(f"Found run: {self.run.id}")        
        
        wait_for_successful_run_completion(run_pathspec=f"{self.run.parent.id}/{self.run.id}", poll_interval=10)
        return self.run

    @step
    def run_a1_paramset1(self):
        # The runtime task's job is to 
        # - call flow A1 with some params
        # - store the relevant artifacts in S3 using self. 
        #   https://docs.metaflow.org/metaflow/basics#artifacts
        self.run = self._operate_a1_run(self.paramset1) # run is completed.
        if self.run is None:
            raise RuntimeError("FlowA1 run not found or failed to locate - cannot proceed")
        self.result = self.run['end'].task.data.result
        self.next(self.aggregate_and_do_work)

    @step
    def run_a1_paramset2(self):
        self.run = self._operate_a1_run(self.paramset2)
        if self.run is None:
            raise RuntimeError("FlowA1 run not found or failed to locate - cannot proceed")
        self.result = self.run['end'].task.data.result
        self.next(self.aggregate_and_do_work)
    
    @step
    def aggregate_and_do_work(self, inputs):
        self.df = {
            "run_a1_paramset1": inputs.run_a1_paramset1.result,
            "run_a1_paramset2": inputs.run_a1_paramset2.result,
        }
        self.result = sum([v if v is not None else 0 for v in self.df.values()]) / len(self.df)
        self.next(self.end)

    @step
    def end(self):
        print(f"B1Flow is ending with df: {self.df}")
        print(f"B1Flow is ending with result: {self.result}")

if __name__ == "__main__":
    FlowB1()