# Config Orion via CLI

This command-line tool provides a simple way to config an orion database server.

## Building the tool
1. Run from `orion-sdk` root folder
2. Run `make binary` to create an executable file named bcdbadmin under `bin` directory.


## Commands

Here we list and describe the available commands.
We give a short explanation of their usage and describe the flags for each command.
We provide real-world examples demonstrating how to use the CLI tool for various tasks.



### Version Command
This command prints the version of the CLI tool.
1. Run from `orion-sdk` root folder.
2. Run `./bin/bcdbadmin version`. This command has no flags.



### Config Command
This command enables to config an orion server or ask for the configuration of an orion server. 

###
<a id="get_config_command"></a>
#### Get Config Command
1. Run from 'orion-sdk' root folder.
2. For Get Config Run `bin/bcdbadmin config get [args]`.

   Replace `[args]` with flags.

###
##### Flags
| Flags                             | Description                                                                   |
|-----------------------------------|-------------------------------------------------------------------------------|
| `-d, --db-connection-config-path` | the absolute or relative path of CLI connection configuration file            |
| `-c, --cluster-config-path`       | the absolute or relative path to which the server configuration will be saved |

Both flags are necessary flags. If any flag is missing, the cli will raise an error.

###
##### Example:

Running 
`bin/bcdbadmin config get -d "connection-session-config.yaml" -c "local/config"`
reads the connection and session details needed for connecting to a server from `connection-session-config.yaml` and 
sends a config TX.
It creates directories in `local/config` with the respective certificates, a yaml file, named `shared_cluster_config.yml`, that includes the cluster configuration
and a yaml file, named `version.yml`, that includes the version.


###
<a id="get_last_config_block_command"></a>
#### Get Last Config Block Command
1. Run from 'orion-sdk' root folder.
2. For Get last config block Run `bin/bcdbadmin config getLastConfigBlock [args]`.

   Replace `[args]` with flags.

###
##### Flags
| Flags                             | Description                                                                |
|-----------------------------------|----------------------------------------------------------------------------|
| `-d, --db-connection-config-path` | the absolute or relative path of CLI connection configuration file         |
| `-c, --cluster-config-path`       | the absolute or relative path to which the last config block will be saved |

Both flags are necessary flags. If any flag is missing, the cli will raise an error.

###
##### Example:

Running
`bin/bcdbadmin config getLastconfigBlock -d "connection-session-config.yaml" -c "local/config"`
reads the connection and session details needed for connecting to a server from `connection-session-config.yaml` and
sends a config TX.
It creates a yaml file, named `last_config_block.yml`, under the `local/config` directory.


###
<a id="get_cluster_status_command"></a>
#### Get Cluster Status Command
1. Run from 'orion-sdk' root folder.
2. For Get last config block Run `bin/bcdbadmin config getClusterStatus [args]`.

   Replace `[args]` with flags.

###
##### Flags
| Flags                             | Description                                                                |
|-----------------------------------|----------------------------------------------------------------------------|
| `-d, --db-connection-config-path` | the absolute or relative path of CLI connection configuration file         |

The flag above is a necessary flag. If the flag is missing, the cli will raise an error.

###
##### Example:

Running
`bin/bcdbadmin config getClusterStatus -d "connection-session-config.yaml"`
reads the connection and session details needed for connecting to a server from `connection-session-config.yaml` and
sends a config TX.
It prints the output (the cluster status) to the screen. 


###
<a id="set_config_command"></a>
#### Set Config Command
1. Run from 'orion-sdk' root folder.
2. For Set Config Run:
   2.1 `bin/bcdbadmin config get [args]`.
   2.2 `bin/bcdbadmin config set [args]`.

   Replace `[args]` with corresponding flags. The flags for config get are detailed in the table above.

###
##### Flags
| Flags                             | Description                                                              |
|-----------------------------------|--------------------------------------------------------------------------|
| `-d, --db-connection-config-path` | the absolute or relative path of CLI connection configuration file       |
| `-c, --cluster-config-path`       | the absolute or relative path to the new cluster configuration yaml file |

Both flags are necessary flags. If any flag is missing, the cli will raise an error.

NOTE: the new cluster configuration yaml file should be named as: "new_cluster_config.yml" and should be located in the same directory as the directory given as flag via the GET command.


###
##### Example:

Running 
`bin/bcdbadmin config set -d "connection-session-config.yaml" -c "local/new_cluster_config.yml"`
reads the connection and session details needed for connecting to a server from `connection-session-config.yaml` and 
sends a config TX.
It reads the `local/new_cluster_config.yml` to fetch the new cluster configuration and set it.


###
#### Using the set config command to manage the cluster configuration
In addition to reconfiguring parameters, the above commands can be used to add or remove a node.

The following steps describe how to add a node to the cluster:
1. Run from 'orion-sdk' root folder.
2. Run `bin/bcdbadmin config get [args]` to get the cluster configuration. Replace `[args]` with corresponding flags as detailed above, see [Get Config Command](#get_config_command).
3. Create a new shared configuration file, named `new_cluster_config.yml`, and add the 4th node to the configuration. Make sure to add the node to both the Members list and the Nodes list.

    Note: it is possible to create a new file or to edit the `shared_cluster_config.yml` obtained in the previous step.
4. Run `bin/bcdbadmin config set [args]` to set the new configuration. Replace `[args]` with corresponding flags as detailed above, see [Set Config Command](#set_config_command).
    
    After this step the cluster configuration should contain 4 nodes.
5. Run `bin/bcdbadmin config getLastConfigBlock [args]` to get the last config block. Replace `[args]` with corresponding flags as detailed above, see [Get Last Config Block Command](#get_last_config_block_command).
6. Edit the `config.yml` file of 4th node and change the boostrap method and file:
    ```yaml
    - bootstrap: 
        method: join
        file: [the path for last_config_block.yml file]
    ```
8. Start the 4th node.
9. Run `bin/bcdbadmin config getClusterStatus [args]` to get the cluster status. Replace `[args]` with corresponding flags as detailed above, see [Get Cluster Status Command](#get_cluster_status_command).

    Make sure there are 4 nodes in the resulting configuration.