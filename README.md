# tesira2mqtt

A bridge between [Biamp Tesira](https://www.biamp.com/products/families/tesira) audio interfaces and a mqtt MQTT broker. It connects to the device via SSH and talks the [Tesira Text Protcol (TTP)](https://tesira-software-help.biamp.com/#t=assets%2FTOC%2FSystem_Control%2FTesira_Software_System_Control_Overview.htm) to it.


For each (supported) audio block multiple MQTT topics are published and subscribed to, allowing for changing and reading it's parameters.

### Room Combiner walls

Room Combiner blocks are detected automatically. For a block named `Combiner`, wall state is published as a retained boolean on `tesira2mqtt/Combiner/wall/<wall-number>` and can be changed by publishing `true` or `false` to `tesira2mqtt/Combiner/wall/<wall-number>/set`.

The values map directly to Tesira's `wallState` attribute: `true` means the wall is closed (the rooms are separated), while `false` means it is open (the rooms are combined). The block also publishes `tesira2mqtt/Combiner/num_walls`.

Each room publishes its group number and source assignment as retained integer values:

- `tesira2mqtt/Combiner/room/<room-number>/group` (`/set` sets the Tesira `group` attribute)
- `tesira2mqtt/Combiner/room/<room-number>/source` (`/set` sets the Tesira `sourceSelection` attribute)
- `tesira2mqtt/Combiner/num_rooms`

### Logic State

Logic State blocks are detected automatically. For a block named `Logic`, each channel is published as a retained boolean on `tesira2mqtt/Logic/state/<channel-number>`. Publish `true` or `false` to `tesira2mqtt/Logic/state/<channel-number>/set` to change the state. The block also publishes `tesira2mqtt/Logic/num_channels`.

## Prerequisites

The Tesira device must have it's **Control** Ethernet port connected to the network where this bridge will run, have a static IP or hostname and have SSH enabled.


## Configuration

The service attempts to load `tesira2mqtt.yaml` from the current working directory, `/etc/tesira2mqtt.yaml`. Alternatively the `CONFIG_PATH` environment variable can be set to manually indicate the location of the config.

The structure of the file is as follows:

```yaml
tesira_ssh:
  addr: "10.12.10.104:22" # The IP + port of the tesira device
  username: "default"
  # password: "" # optional
  # password_file: "" # optional, allows to load a password from a secret
  # known_hosts_file: "" # optional, if specified host checking is performed
  # timeout: 25s # Optional
mqtt:
  broker: "127.0.0.1:1883" # MQTT broker IP/hostname + port
  username: "username"
  password: "<password>" 
  # prefix: "tesira2mqtt/" # Optional, must have trailing slash
```

## Deployment

The service is best deployed using docker. An image is provided at `docker pull ghcr.io/alufers/tesira2mqtt:master`


## License 
MIT
