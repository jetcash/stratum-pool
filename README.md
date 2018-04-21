cryptonote-pool
====================

High performance Node.js (with native C addons) mining pool for CryptoNote based coins such as Jetcash and Bytecoin.
Comes with lightweight example front-end script which uses the pool's AJAX API.



#### Table of Contents
* [Features](#basic--features)
* [Community Support](#community--support)
* [Pools Using This Software](#pools-using-this-software)
* [Usage](#usage)
  * [Requirements](#requirements)
  * [Downloading & Installing](#1-downloading--installing)
  * [Configuration](#2-configuration)
  * [Configure Easyminer](#3-optional-configure-cryptonote-easy-miner-for-your-pool)
  * [Starting the Pool](#4-start-the-pool)
  * [Host the front-end](#5-host-the-front-end)
  * [Customizing your website](#6-customize-your-website)
  * [Upgrading](#upgrading)
* [Monitoring Your Pool](#monitoring-your-pool)
* [Donations](#donations)
* [License](#license)


#### Basic features

* TCP (stratum-like) protocol for server-push based jobs
  * Compared to old HTTP protocol, this has a higher hash rate, lower network/CPU server load, lower orphan
    block percent, and less error prone
* IP banning to prevent low-diff share attacks
* Socket flooding detection
* Payment processing
  * Splintered transactions to deal with max transaction size
  * Minimum payment threshold before balance will be paid out
  * Minimum denomination for truncating payment amount precision to reduce size/complexity of block transactions
* Detailed logging
* Ability to configure multiple ports - each with their own difficulty
* Variable difficulty / share limiter
* Share trust algorithm to reduce share validation hashing CPU load
* Clustering for vertical scaling
* Modular components for horizontal scaling (pool server, database, stats/API, payment processing, front-end)
* Live stats API (using AJAX long polling with CORS)
  * Currency network/block difficulty
  * Current block height
  * Network hashrate
  * Pool hashrate
  * Each miners' individual stats (hashrate, shares submitted, pending balance, total paid, etc)
  * Blocks found (pending, confirmed, and orphaned)
* An easily extendable, responsive, light-weight front-end using API to display data

#### Extra features

* Admin panel
  * Aggregated pool statistics
  * Coin daemon & wallet RPC services stability monitoring
  * Log files data access
  * Users list with detailed statistics
* Historic charts of pool's hashrate and miners count, coin difficulty, rates and coin profitability
* Historic charts of users's hashrate and payments
* Miner login(wallet address) validation
* Five configurable CSS themes
* Set fixed difficulty on miner client by passing "address" param with ".[difficulty]" postfix
* Prevent "transaction is too big" error with "payments.maxTransactionAmount" option


### Community / Support

* [CryptoNote Technology](https://cryptonote.org)
* [CryptoNote Forum](https://forum.cryptonote.org/)

#### Pools Using This Software

* https://pool.jetcash.org

Usage
===

#### Requirements
* Coin daemon(s) (find the coin's repo and build latest version from source)
* [Node.js](http://nodejs.org/) v5+ ([follow these installation instructions](https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager))
* [Redis](http://redis.io/) key-value store v2.6+ ([follow these instructions](http://redis.io/topics/quickstart))
* libssl required for the node-multi-hashing module
* For Ubuntu: `sudo apt-get install libssl-dev`


##### Seriously
Those are legitimate requirements. If you use old versions of Node.js or Redis that may come with your system package manager then you will have problems. Follow the linked instructions to get the last stable versions.


[**Redis security warning**](http://redis.io/topics/security): be sure firewall access to redis - an easy way is to
include `bind 127.0.0.1` in your `redis.conf` file. Also it's a good idea to learn about and understand software that
you are using - a good place to start with redis is [data persistence](http://redis.io/topics/persistence).

##### Easy install on Ubuntu 14 LTS
Installing pool on different Linux distributives is different because it depends on system default components and versions. For now the easiest way to install pool is to use Ubuntu 14 LTS. Thus, all you had to do in order to prepare Ubunty 14 for pool installation is to run:

```bash
sudo apt-get install git redis-server libboost1.62-all-dev nodejs-dev nodejs-legacy npm cmake libssl-dev
```

#### 1) Downloading & Installing

Clone the repository and run `npm update` for all the dependencies to be installed:

```bash
git clone https://github.com/jetcash/stratum-pool.git pool
cd pool
npm install
```

#### 2) Configuration


Explanation for each field:
```javascript
/* Used for storage in redis so multiple coins can share the same redis instance. */
"coin": "jetcash",

/* Used for front-end display */
"symbol": "JTC",

/* Minimum units in a single coin, see COIN constant in DAEMON_CODE/src/CryptoNoteConfig.hpp */
"coinUnits": 100000000000,

/* Coin network time to mine one block, see DIFFICULTY_TARGET constant in DAEMON_CODE/src/CryptoNoteConfig.hpp */
"coinDifficultyTarget": 120,

"logging": {

    "files": {

        /* Specifies the level of log output verbosity. This level and anything
           more severe will be logged. Options are: info, warn, or error. */
        "level": "info",

        /* Directory where to write log files. */
        "directory": "logs",

        /* How often (in seconds) to append/flush data to the log files. */
        "flushInterval": 5
    },

    "console": {
        "level": "info",
        /* Gives console output useful colors. If you direct that output to a log file
           then disable this feature to avoid nasty characters in the file. */
        "colors": true
    }
},

/* Modular Pool Server */
"poolServer": {
    "enabled": true,

    /* Set to "auto" by default which will spawn one process/fork/worker for each CPU
       core in your system. Each of these workers will run a separate instance of your
       pool(s), and the kernel will load balance miners using these forks. Optionally,
       the 'forks' field can be a number for how many forks will be spawned. */
    "clusterForks": "auto",

    /* Address where block rewards go, and miner payments come from. */
    "poolAddress": "J7wm4FYrp5kRV29Pm1pGfVdz8rqSiBFghXo4Tkh7et4y714kRJjndT2FEiF2vfGmSeFK6VmXQevy9BMx3bPrHFK2RVqF43P"

    /* Poll RPC daemons for new blocks every this many milliseconds. */
    "blockRefreshInterval": 1000,

    /* How many seconds until we consider a miner disconnected. */
    "minerTimeout": 900,

    "ports": [
        {
            "port": 3333, //Port for mining apps to connect to
            "difficulty": 100, //Initial difficulty miners are set to
            "desc": "Low end hardware" //Description of port
        },
        {
            "port": 5555,
            "difficulty": 2000,
            "desc": "Mid range hardware"
        },
        {
            "port": 7777,
            "difficulty": 10000,
            "desc": "High end hardware"
        }
    ],

    /* Variable difficulty is a feature that will automatically adjust difficulty for
       individual miners based on their hashrate in order to lower networking and CPU
       overhead. */
    "varDiff": {
        "minDiff": 2, //Minimum difficulty
        "maxDiff": 100000,
        "targetTime": 100, //Try to get 1 share per this many seconds
        "retargetTime": 30, //Check to see if we should retarget every this many seconds
        "variancePercent": 30, //Allow time to very this % from target without retargeting
        "maxJump": 100 //Limit diff percent increase/decrease in a single retargetting
    },

    /* Set difficulty on miner client side by passing <address> param with .<difficulty> postfix
       minerd -u JAV3SXv8KrYLqfzVqFovYp3xcwNtqkKJbTNHHxKJEreUcXHZf3MGPTwaZFvWyxpSLGd384YKThfni8ogGG31jt2D13TGuWd.5000 */
    "fixedDiff": {
        "enabled": true,
        "separator": ".", // character separator between <address> and <difficulty>
    },

    /* Feature to trust share difficulties from miners which can
       significantly reduce CPU load. */
    "shareTrust": {
        "enabled": true,
        "min": 10, //Minimum percent probability for share hashing
        "stepDown": 3, //Increase trust probability % this much with each valid share
        "threshold": 10, //Amount of valid shares required before trusting begins
        "penalty": 30 //Upon breaking trust require this many valid share before trusting
    },

    /* If under low-diff share attack we can ban their IP to reduce system/network load. */
    "banning": {
        "enabled": true,
        "time": 600, //How many seconds to ban worker for
        "invalidPercent": 25, //What percent of invalid shares triggers ban
        "checkThreshold": 30 //Perform check when this many shares have been submitted
    }
},

/* Module that sends payments to miners according to their submitted shares. */
"payments": {
    "enabled": true,
    "interval": 600, //how often to run in seconds
    "maxAddresses": 50, //split up payments if sending to more than this many addresses
    "transferFeePerCoin": 1000000, //fee to pay for each transaction
    "minPayment": 100000000000, //miner balance required before sending payment
    "maxTransactionAmount": 100000000000000000, //split transactions by this amount(to prevent "too big transaction" error)
    "denomination": 100000000000 //truncate to this precision and store remainder
},

/* Module that monitors the submitted block maturities and manages rounds. Confirmed
   blocks mark the end of a round where workers' balances are increased in proportion
   to their shares. */
"blockUnlocker": {
    "enabled": true,
    "interval": 30, //how often to check block statuses in seconds

    /* Block depth required for a block to unlocked/mature. Found in daemon source as
       the variable CRYPTONOTE_MINED_MONEY_UNLOCK_WINDOW */
    "depth": 60,
    "poolFee": 1,
    "devDonation": 0.2,
    "devAddress": "JAV3SXv8KrYLqfzVqFovYp3xcwNtqkKJbTNHHxKJEreUcXHZf3MGPTwaZFvWyxpSLGd384YKThfni8ogGG31jt2D13TGuWd"
},

/* AJAX API used for front-end website. */
"api": {
    "enabled": true,
    "hashrateWindow": 600, //how many second worth of shares used to estimate hash rate
    "updateInterval": 3, //gather stats and broadcast every this many seconds
    "port": 8117,
    "blocks": 30, //amount of blocks to send at a time
    "payments": 30, //amount of payments to send at a time
    "password": "test" //password required for admin stats
},

/* Coin daemon connection details. */
"daemon": {
    "host": "127.0.0.1",
    "port": 12021
},

/* Wallet daemon connection details. */
"wallet": {
    "host": "127.0.0.1",
    "port": 12010
},

/* Redis connection into. */
"redis": {
    "host": "127.0.0.1",
    "port": 6379
}

/* Monitoring RPC services. Statistics will be displayed in Admin panel */
"monitoring": {
    "daemon": {
        "checkInterval": 60, //interval of sending rpcMethod request
        "rpcMethod": "get_status" //RPC method name
    },
    "wallet": {
        "checkInterval": 60,
        "rpcMethod": "get_balance"
    }

/* Collect pool statistics to display in frontend charts  */
"charts": {
    "pool": {
        "hashrate": {
            "enabled": true, //enable data collection and chart displaying in frontend
            "updateInterval": 60, //how often to get current value
            "stepInterval": 1800, //chart step interval calculated as average of all updated values
            "maximumPeriod": 86400 //chart maximum periods (chart points number = maximumPeriod / stepInterval = 48)
        },
        "workers": {
            "enabled": true,
            "updateInterval": 60,
            "stepInterval": 1800, //chart step interval calculated as maximum of all updated values
            "maximumPeriod": 86400
        },
        "difficulty": {
            "enabled": true,
            "updateInterval": 1800,
            "stepInterval": 10800,
            "maximumPeriod": 604800
        },
        "price": { //USD price of one currency coin received from cryptonator.com/api
            "enabled": true,
            "updateInterval": 1800,
            "stepInterval": 10800,
            "maximumPeriod": 604800
        },
        "profit": { //Reward * Rate / Difficulty
            "enabled": true,
            "updateInterval": 1800,
            "stepInterval": 10800,
            "maximumPeriod": 604800
        }
    },
    "user": { //chart data displayed in user stats block
        "hashrate": {
            "enabled": true,
            "updateInterval": 180,
            "stepInterval": 1800,
            "maximumPeriod": 86400
        },
        "payments": { //payment chart uses all user payments data stored in DB
            "enabled": true
        }
    }
```

#### 3) [Optional] Configure cryptonote-easy-miner for your pool
Your miners that are Windows users can use [cryptonote-easy-miner](https://github.com/zone117x/cryptonote-easy-miner)
which will automatically generate their wallet address and stratum multiple threads of simpleminer. You can download
it and edit the `config.ini` file to point to your own pool.
Inside the `easyminer` folder, edit `config.init` to point to your pool details
```ini
pool_host=example.com
pool_port=5555
```

Rezip and upload to your server or a file host. Then change the `easyminerDownload` link in your `config.json` file to
point to your zip file.

#### 4) Start the pool

```bash
node init.js
```

The file `config.json` is used by default but a file can be specified using the `-config=file` command argument, for example:

```bash
node init.js -config=config_backup.json
```

This software contains four distinct modules:
* `pool` - Which opens ports for miners to connect and processes shares
* `api` - Used by the website to display network, pool and miners' data
* `unlocker` - Processes block candidates and increases miners' balances when blocks are unlocked
* `payments` - Sends out payments to miners according to their balances stored in redis


By default, running the `init.js` script will start up all four modules. You can optionally have the script start
only start a specific module by using the `-module=name` command argument, for example:

```bash
node init.js -module=api
```

#### 5) Host the front-end

Simply host the contents of the `website_example` directory on file server capable of serving simple static files.


Edit the variables in the `website_example/config.js` file to use your pool's specific configuration.
Variable explanations:

```javascript

/* Must point to the API setup in your config.json file. */
var api = "http://pool.jetcash.org:8117";

/* Pool server host to instruct your miners to point to.  */
var poolHost = "pool.jetcash.org";

/* IRC Server and room used for embedded KiwiIRC chat. */
var irc = "irc.freenode.net/#jetcash";

/* Contact email address. */
var email = "pool@jetcash.org";

/* Market stat display params from https://www.cryptonator.com/widget */
var cryptonatorWidget = ["JTC-BTC", "JTC-USD", "JTC-EUR"];

/* Any custom CSS theme for pool frontend */
var themeCss = "themes/default-theme.css";

```

#### 6) Customize your website

The following files are included so that you can customize your pool website without having to make significant changes
to `index.html` or other front-end files thus reducing the difficulty of merging updates with your own changes:
* `custom.css` for creating your own pool style
* `custom.js` for changing the functionality of your pool website


Then simply serve the files via nginx, Apache, Google Drive, or anything that can host static content.


#### Upgrading
When updating to the latest code its important to not only `git pull` the latest from this repo, but to also update
the Node.js modules, and any config files that may have been changed.
* Inside your pool directory (where the init.js script is) do `git pull` to get the latest code.
* Remove the dependencies by deleting the `node_modules` directory with `rm -r node_modules`.
* Run `npm update` to force updating/reinstalling of the dependencies.
* Compare your `config.json` to the latest example ones in this repo or the ones in the setup instructions where each config field is explained. You may need to modify or add any new changes.

### Monitoring Your Pool

* To inspect and make changes to redis I suggest using [redis-commander](https://github.com/joeferner/redis-commander)
* To monitor server load for CPU, Network, IO, etc - I suggest using [New Relic](http://newrelic.com/)
* To keep your pool node script running in background, logging to file, and automatically restarting if it crashes - I suggest using [forever](https://github.com/nodejitsu/forever)


Donations
---------
* BTC: `19qNeqb3w6WcYLemzoHVvU9x11qPDKaCUT`
* ETH: `0xae5009e1be355fa7695cf5509bc30ff238a22839`

License
-------
Released under the GNU General Public License v2

http://www.gnu.org/licenses/gpl-2.0.html
