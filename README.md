# accountServerConnector

Get PTC accounts from external accounts server - intended for use with [pogoAccountServer](https://github.com/crhbetz/pogoAccountServer).
A device will fetch a PTC account from the server upon every PTC login. The recommended pogoAccountServer decides which account to serve, detailed in the README there.

# Setup

* upload .mp file from releases to MADmin or clone the repo into `MAD/plugins/accountServerConnector` (leave out the `mp-` part of the directory - yes,
this is annoying :-) )
* `cp plugin.ini.example plugin.ini` and configure the `plugin.ini` according to your setup
* restart MAD
