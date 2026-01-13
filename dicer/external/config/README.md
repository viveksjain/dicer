## README
This directory contains the configuration for Dicer Targets. A
target configuration is required before external clients
can use Dicer.

This directory is organized into subdirectories for each environment
i.e. `dev`, `staging`, and `prod`. To use
Dicer in a given environment make sure to add or update
configuration in the corresponding directory.

### Instructions on the target configuration file
The target configuration file uses textproto format. For documentation
on the fields, see [target.proto](../proto/target.proto).
Please create a file named after the [Target's name](../../../docs/UserGuide.md),
with the _.textproto_ extension, and place it in the appropriate environment directory. For
example, for the _foo_ service in the _dev_ environment, the file should be
named _dev/foo.textproto_.

For an example, see the [demo-cache-app textproto](dev/demo-cache-app.textproto).

The header of the configuration file must be

```
# proto-file: dicer/external/proto/target.proto
# proto-message: TargetConfigP
```
which defines the target configuration proto definition file and message.
