I want to rewrite the entire go/provisioner/local package. The changes will be isolated there, except for maybe adding
more ComponentType values into ID in proto/clustermetadata.proto

Here are the key points of the new design:

- Provisioning resources means:
  - identifying datastores that need to be initialized, services that need to be started, or already-created datastores & services that can be reused
  - when starting a service, saving information to disk so that we can map the resource ID back to a process ID later to help terminate (deprovision)
    the service
  - identifying from saved service information on disk if there may be services running in a cluster that aren't requested in the LocalProvisionerConfig. These are orphan services that should be deprovisioned, even in the Provision operation
  - There should roughly be a resource provisioner per ComponentType. Each type-specific resource provisioner is presumed to depend on the global
    topology (except for the global topology resource itself) and cell resources are presumed to also depend on the cell's topology (except for
    the cell topology itself, which only depends on the global topology). The relevant topology will always be provisioned before any resources
    that depend on them, and a provisioner will be provided with a ProvisionContext at provision time that can be used to open a connection to
    the local or global topology server or access etcd connection information to pass into command lines to subprocesses. Note: topo.GlobalCell is a sentinel value used as the cell for resources that are global
  - Provision methods will not print anything: all provisioning will start massively parallel (respecting dependencies, though, like topology server
    before things that need it). Displaying status of cluster startup will be managed by a separate function which can monitoring the provisioning
    process and show the status

- Deprovisioning resources means:
  - looking at saved service information on disk to identify running services that may need to be stopped. The stopping process will often
    look like terminating a process, although in some cases it may be more complex: multipooler and pgctld are provisioned as a single resource,
    but when it's time to stop them it's possible that either one could be running without the other. While multipooler can be tracked as a process
    ID that can be terminated, pgctld will be terminated based on its data directory location using "pg_ctl stop". The multipooler is not deprovisioned
    until both the multipooler process is stopped and the postgres process is terminated

So for example, a provisioner might have bits of code that look like this:

func (m* MultipoolerResource) Provision(ctx context.Context, pCtx ProvisionContext) *ProvisionResult {
state, err := pCtx.readState() // Find existing LocalProvisionedService for this resource
if err != nil {
if (state.PID > 0) {
// There's already a running instance with both pgctld and multipooler started
return state
}
} else {
state = &LocalProvisionedService{
// multipooler service, but without the PID (we haven't started it yet)
}
}

// Save state early to make sure we'll deprovision pgctld if needed
pCtx.saveState(state)

// Runs initdb and "pgctld start" with the data directory for this provisioner
m.startPgctld()

multipoolerCmd := exec.CommandContext(ctx, multipoolerBinary, args...)
multipoolerCmd.Start();

state.PID = multipoolerCmd.Process.PID
pCtx.saveState(state)

return &provisioner.ProvisionResult{
// ...
}
}

func (m\* MultipoolerResource) Deprovision(ctx context.Context, pCtx ProvisionContext) error {
state, err := pCtx.readState() // Find existing LocalProvisionedService for this resource
if err != nil {
return fmt.Errorf("internal error: can't deprovision a resource that has no state", err)
}

if (state.PID) {
// terminate the multipooler process
}

// Run pgctld stop to terminate postgres & pgctld

// If no error is returned, it means that the state file will be deleted by the provisioning orchestrator from disk after Deprovision() returns
}

Or for provisioning a cell, maybe it'd look something like this:

func (m* CellResource) Provision(ctx context.Context, pCtx ProvisionContext) *ProvisionResult {
ts, err := pCtx.OpenGlobalTopo()
if err != nil {
return fmt.Errorf("failed to connect to topology server: %w", err)
}
defer ts.Close()

    // Check if cell already exists
    _, err = ts.GetCell(ctx, cellName)
    // ... create the cell if necessary

}

I'm not strongly attached to how status is printed during provisioning, but I don't want it to feel relatively stable:
things shouldn't print in random different order every time you start a cluster. Ideally I'd want state to print
in a consistent, senseable order but show progress as it happens to the degree that it's practical

How does all this sound? My hope is that it'll make the local provisioner:

- Simpler
- More easily extended with new resource types
- Faster (parallel provisioing)
- More reliable (like deprovisioning running processes during "multigres cluster start" if those processes are no longer in the config)
