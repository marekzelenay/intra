# PowerBot Base Algorithm

The base class for all PowerBot algorithms.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install PowerBot Base Algorithm.

```bash
pip install git+https://github.com/powerbot-trading/powerbot_algorithm_base_class.git
```

## Usage

Sub-class the `Algorithm` base class and override the `algorithm()` method.
This will cause the algorithm to get triggered on a per-contract basis, defined by its delivery_start and delivery_end.
the passed `Trigger` object contains all configured events that have been received for this time span since the last run.
These triggers can be read in `dict` form from the corresponding stores (`trigger.obce, .own_orders, .own_trades, .internal_trades, .signals`)
or as a full list via `trigger.all()`.

```python
from algorithm_base_class.algorithm import Algorithm, Trigger


class YourAlgo(Algorithm):
    async def algorithm(self, trigger: Trigger):
        pass  # your logic here
```

It is also possible to run on a per-timeslot basis. To use this feature, override the `__init__()` method and set the
`timeslot_minutes` parameter to the desired size (must be a multiple of 5).
This will collect triggers for all contracts (at least partially) within a given timeslot inside one `Trigger` object.
While this will prevent contracts shorter than the configured timeslot to run in parallel, it conversely allows contracts
longer than the configured timeslot to do so.

When run stand-alone, the parameter can also be passed directly to the constructor.

````python
class YourAlgo(Algorithm):
    def __init__(self):
        super().__init__(timeslot_minutes=60)

    async def algorithm(self, trigger: Trigger):
        pass  # your logic here
````

For code that should be executed only once at start-up, but after the server-side config has been fetched,
the base class provides an overridable method `setup()`. Similarly, for code that is supposed to be executed when the algorithm is shut down the base
class offers a method `teardown()`.

````python
class YourAlgo(Algorithm):
    async def setup(self):
        pass  # Your setup logic here

    async def teardown(self):
        pass  # Your teardown logic here
````

The algorithm base class is designed to be started within the context of
the R2D2 framework,
but can be started manually for testing purposes or stand-alone operation.

## Configuration

Config parameters are fetched from the algorithm endpoint of PowerBot.
It is therefore required that the algorithm and its instances be registered at the server beforehand (via the REST API).

There are two kinds of server-side algorithm parameters:

Lower case parameters (by convention) control the algorithm base class and must therefore be set for EVERY algorithm instance.

* (deprecated) **delivery_area** - The delivery area EIC , e.g. `10YDE-RWENET---I`
*  **delivery_areas** - The list of delivery areas EIC's, e.g. [`10YDE-RWENET---I`, `10YAT-APG------L`]
* (optional) **autorun_seconds** - Triggers the algorithm for all active contracts every n seconds, e.g. `60` defaults to `0` (disabled)
* (optional) **pure_autorun** - Can be used only in combination with **autorun_seconds**, instead of triggering the algorithm for all active contracts, the algorithm will be triggered only once
every n seconds. Cannot be combined with other subscription topics.
* (optional) **schedules** - Accepts **list** of cron-likes [syntax](https://crontab.guru/) entries to trigger the algorithm at specified intervals. The schedule will be evaluated based on the provided timezone.

Here is an example of a cron expression to run a task every day at 13:00 (1:00 PM):


0 13 * * *

Breakdown of the Expression:

    0 → Minute (0th minute of the hour)
    13 → Hour (13:00 or 1 PM)
    * → Day of the month (every day)
    * → Month (every month)
    * → Day of the week (any day)


* (optional) **schedule_timezone** - Timezone to use for the `scheduled` execution. Defaults to `UTC`. Allowed values are listed under `TZ identifier` column [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

Set the schedule_timezone to Europe/Vienna so that, according to the previous example, the algorithm will be triggered at 13:00 local Vienna time.


* (optional) **max_concurrent** - Set the number of concurrent algorithm runs, e.g. `10`. Default value is `1`.
* (optional) **topics** - The list of topics the websocket connection should subscribe to.
* (optional) **delivery_durations** - The list of delivery durations (in minutes e.g. `[15, 60]`) that defines, which contracts should be passed to the `Trigger`, others are ignored. If left
* empty, all contracts will be passed to the `Trigger`.

Upper case parameters (by convention) are used for user-defined, algorithm-specific settings.

* **SOME_USER_PARAM** - eg. some_value
* **MAX_NET_POS** - eg. 1000
* ...

### Topics

The optional config parameter `topics` expects a list of strings of websocket topics to subscribe to.
Defaults to all supported, i.e. `["orderbookchangedevent", "ownorders", "owntrades", "signals", "orderbooksnapshots", "publictrades", "areaorderbookchangedevent"]`
To avoid the possibility of unintentional simultaneous algorithm runs for the same contract the `orderbookgroup` subscription cannot be used in
combination with other topics.
To avoid simultaneous subscriptions to order book changes specific to delivery areas accessible to a portfolio (`"orderbookchangedevent"`) and
order book changes specific to the delivery areas set for an algorithm instance (`"areaorderbookchangedevent"`), these topics cannot be used in combination.

Topics and their corresponding objects passed via `Trigger`:

* **orderbookchangedevent** - `OrderBookChangedEvent`
* **areaorderbookchangedevent** - `OrderBookChangedEvent`
* **ownorders** - `OwnOrderChanges`
* **owntrades** - `OwnTradeChanges`
* **signals** - `SignalChanges`
* **orderbooksnapshots** - `OrderBookChanges`
* **orderbookgroup** - `OrderBookGroup`
* **publictrades** - `PublicTradeChanges`

## Deployment

### Running with an orchestration service (production mode)

It is recommended to use an orchestration service to deploy algorithms that are build upon the base class. For this purpose, PowerBot offers a
Kubernetes-based orchestrator R2D2, which enables high availability for algo deployment.

### Running stand-alone (development mode)

When run outside an orchestration framework, e.g. during development, the following environment variables are required.
These variables would normally be passed down from the framework.

* **ALGORITHM_ID** - The algorithm ID as registered at the server, e.g. `TestAlgo`
* **INSTANCE_ID** - The specific instance ID as registered at the server, e.g. `Instance1`
* **API_KEY** - An API key for the given PowerBot instance, e.g. `xxx-xxx-xxx-xxxx`
* **URL_API** - The URL of the API, e.g. `https://example.powerbot-trading.com:443/example/epex/v2/api`
* **URL_WEBSOCKET** - The URL of the websocket connection, e.g. `wss://example.powerbot-trading.com/test/example/v2/subscription`
* (optional) **ALGO_LOG_LEVEL** - Defines the log-level, can be any of `CRITICAL|ERROR|WARNING|INFO|DEBUG` defaults to `INFO`
* (optional) **ALGO_LOG_ECS** - Enable ECS-style JSON logging. Can be `1|0` defaults to `0`

In the folder `examples` you find examples how to build algorithms on top of the base class.
