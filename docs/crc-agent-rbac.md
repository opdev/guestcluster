# CRC agent permissions

Each CRC ClusterInstance has one ServiceAccount and one RoleBinding in its
namespace. The manager creates them before it creates an agent Job. The binding
refers to the installed `crc-agent-instance-role` ClusterRole. Its subject is
the instance account in the same namespace. An instance keeps these resources
through Job retries and VMI replacement. During deletion, the manager waits for
the Jobs to stop before it removes the binding and account. The instance owner
reference also lets Kubernetes remove them if the instance is deleted.
Direct installs include the ClusterRole. For OLM installs, the manager creates
the same fixed role when it is absent. It checks the role rules before it
creates a binding. Kustomize and the OLM deployment pass the installed role
name to the manager through `CRC_AGENT_CLUSTER_ROLE`.

The account can read and watch VirtualMachineInstances and get, create, and
update Secrets **throughout its namespace**. Separate accounts control the
lifetime of access, but do not limit Secret access to one instance. Limiting
access to named Secrets needs a different agent handoff protocol.

Earlier Jobs keep their immutable Pod templates and use the old shared account.
The old account, Role, and RoleBinding remain installed so those Jobs can
finish. Remove the old objects only after no legacy Jobs use the account.
`CRC_AGENT_SERVICE_ACCOUNT` still names that legacy account.
